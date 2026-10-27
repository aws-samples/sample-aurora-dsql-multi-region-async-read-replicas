"""End-to-end consumer pipeline (single stream, M1 + M3).

Flow per shard:

    Kinesis GetRecords
        │  bytes
        v
    envelope.parse                   ← EnvelopeError → log + skip (DLQ in M3+)
        │  CdcRecord
        v
    FragmentReassembler.ingest       ← CrcMismatch / sweep timeouts → log + skip
        │  Full | AssembledRecord | None
        v
    TransactionReassembler.ingest    ← sweep timeouts → log + skip
        │  CompletedTransaction | None
        v
    OrderingBuffer.ingest            ← BackpressureError → brief sleep + retry

A single apply-runner task drains the ordering buffer in `source.ts_ns` order and hands
each CompletedTransaction to the serial Applier.

M3 — DLQ fence model :
On permanent apply failure, the runner opens a fence in target DSQL (via DlqWriter):
fence row + full payload row + SNS `opened` notification. `OrderingBuffer.set_fence()`
halts commits past the failing `ts_ns`; new records keep flowing in and buffering. A
separate `_fence_poller` task periodically re-reads `__replicator_dlq_fences`; when the
oldest OPEN fence is gone (operator ran `ops dlq retry`/`skip`), the in-memory fence is
cleared and the applier drains the buffered backlog in strict `ts_ns` order.

M1 scope still applies: single source stream, no checkpoint persistence (we always start
from LATEST). Multi-shard horizontal scaling lands in M4.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

import aioboto3
import psycopg
import structlog

from dsql_replicator.applier import Applier, ApplyError
from dsql_replicator.applier.connection import DsqlEndpoint, DsqlTokenCache, open_connection
from dsql_replicator.config import Config
from dsql_replicator.dlq import DlqWriter
from dsql_replicator.envelope import parse
from dsql_replicator.errors import EnvelopeError, ReplicatorError
from dsql_replicator.metrics import MetricPoint, MetricsEmitter
from dsql_replicator.ordering import OrderingBuffer
from dsql_replicator.ordering.buffer import BackpressureError
from dsql_replicator.reassembler import CrcMismatchError, FragmentReassembler, TransactionReassembler
from dsql_replicator.state import CheckpointRepo, LeaseRepo

if TYPE_CHECKING:
    pass

log = structlog.get_logger("consumer")


class ConsumerPipeline:
    """One pipeline instance per (source stream, target cluster) pair."""

    def __init__(
        self,
        cfg: Config,
        *,
        stream_name: str,
        target_endpoint: DsqlEndpoint,
        start_from_horizon: bool = False,
    ) -> None:
        self._cfg = cfg
        self._stream_name = stream_name
        self._target = target_endpoint
        self._start_from_horizon = start_from_horizon

        self._stop = asyncio.Event()
        self._ordering = OrderingBuffer(
            idle_shard_timeout_seconds=cfg.watermark_idle_shard_timeout_ms / 1000.0,
            clock_skew_budget_seconds=cfg.watermark_clock_skew_budget_ms / 1000.0,
            max_depth=cfg.ordering_buffer_max_depth,
        )
        self._applier = Applier(
            target_endpoint,
            max_retries=cfg.apply_max_retries,
            backoff_initial_s=cfg.apply_backoff_initial_ms / 1000.0,
            stripe_count=cfg.applier_stripes,
            worker_count=cfg.applier_workers,
        )
        self._dlq = DlqWriter(cfg, target_endpoint)
        self._fence_poll_interval_s = cfg.fence_poll_interval_ms / 1000.0
        # DSQL CDC distributes a single source transaction's records across every open
        # shard. Reassembly must be process-global, not per-shard, or ~90% of txns on a
        # multi-shard stream are dropped as `txn.timeout` with `buffered < expected`.
        self._txn = TransactionReassembler(
            timeout_seconds=cfg.txn_assembly_timeout_ms / 1000.0,
        )
        self._txn_sweep_interval_s = max(0.25, cfg.txn_assembly_timeout_ms / 1000.0 / 4)
        # Expose stream_name for the multi-stream orchestrator in consumer/__init__.py.
        self.stream_name = stream_name

        # State plane (M4): own connection for CheckpointRepo + LeaseRepo. Writes are
        # low-frequency (1 upsert per shard every ~1s + heartbeat every 10s) so a single
        # shared connection with a lock is cheaper than one per shard.
        self._state_token_cache = DsqlTokenCache(target_endpoint)
        self._state_conn: psycopg.AsyncConnection[tuple[object, ...]] | None = None
        self._state_lock = asyncio.Lock()
        self._task_id = cfg.task_id
        self._lease_ttl_s = cfg.lease_ttl_seconds
        self._lease_heartbeat_s = cfg.lease_heartbeat_seconds
        self._checkpoint_flush_s = cfg.checkpoint_flush_interval_ms / 1000.0

        # Running counters surfaced to logs. CloudWatch wiring lands in M5.
        self.records_received = 0
        self.transactions_committed = 0
        self.rows_replicated = 0
        self.rows_deduped = 0
        # Rows the LWW stamp rejected because an older `t_commit` arrived
        # after a newer one had already been applied for the same key.
        # Counted + persisted to `__replicator_late_discards` on the apply
        # side; emitted as the `RowsLateDiscarded` CloudWatch metric so
        # dashboards can distinguish this from crash-replay dedup and from
        # genuine apply failures.
        self.rows_late_discarded = 0
        self.parse_errors = 0
        self.apply_errors = 0
        self.fences_opened = 0
        self.fences_closed_observed = 0
        self.leases_acquired = 0
        self.leases_lost = 0
        self.checkpoints_flushed = 0
        self.txn_timeouts = 0

        # M5: metrics. Shard-level state updated by the shard worker.
        # When running under the multi-process supervisor (`PROCESS_ID` set in each
        # child), tag emissions with that id so dashboard aggregation can Sum or Avg
        # across processes and alarms can scope to one child when needed.
        process_id = os.environ.get("PROCESS_ID")
        default_dims: tuple[tuple[str, str], ...] = (
            ("TargetCluster", target_endpoint.cluster_identifier or "unknown"),
        )
        if process_id is not None:
            default_dims = (*default_dims, ("ProcessId", process_id))
        self._metrics_interval_s = cfg.metrics_interval_ms / 1000.0
        self._metrics = MetricsEmitter(
            namespace=cfg.metrics_namespace,
            region=cfg.control_region,
            default_dims=default_dims,
        )
        self._shard_state: dict[str, dict[str, Any]] = {}
        self._prev_counters: dict[str, int] = {}

    # ---- public entry point ---------------------------------------------

    async def run(self) -> None:
        """Run until stop() is called or SIGINT. Returns on clean shutdown."""
        self._install_signal_handlers()
        await self._applier.start()
        await self._dlq.start()
        self._state_conn = await open_connection(self._target, self._state_token_cache, autocommit=True)
        session = aioboto3.Session()
        try:
            async with session.client("kinesis", region_name=self._cfg.source_region) as kinesis:
                shards = await self._list_shards(kinesis)
                log.info(
                    "consumer.start",
                    stream=self._stream_name,
                    shards=len(shards),
                    target_cluster=self._target.cluster_identifier,
                )
                worker_tasks = [
                    asyncio.create_task(self._shard_worker(kinesis, s), name=f"shard:{s}") for s in shards
                ]
                runner_task = asyncio.create_task(self._apply_runner(), name="apply-runner")
                fence_task = asyncio.create_task(self._fence_poller(), name="fence-poller")
                metrics_task = asyncio.create_task(self._metrics_loop(), name="metrics-loop")
                sweep_task = asyncio.create_task(self._txn_sweeper(), name="txn-sweeper")
                try:
                    await self._stop.wait()
                finally:
                    for t in worker_tasks:
                        t.cancel()
                    runner_task.cancel()
                    fence_task.cancel()
                    metrics_task.cancel()
                    sweep_task.cancel()
                    await asyncio.gather(
                        *worker_tasks,
                        runner_task,
                        fence_task,
                        metrics_task,
                        sweep_task,
                        return_exceptions=True,
                    )
        finally:
            await self._applier.close()
            await self._dlq.close()
            if self._state_conn is not None and not self._state_conn.closed:
                await self._state_conn.close()
            self._state_conn = None
            log.info(
                "consumer.stopped",
                records=self.records_received,
                txns=self.transactions_committed,
                rows=self.rows_replicated,
                dedup=self.rows_deduped,
                parse_errors=self.parse_errors,
                apply_errors=self.apply_errors,
                fences_opened=self.fences_opened,
                fences_closed_observed=self.fences_closed_observed,
                leases_acquired=self.leases_acquired,
                leases_lost=self.leases_lost,
                checkpoints_flushed=self.checkpoints_flushed,
                txn_timeouts=self.txn_timeouts,
            )

    def stop(self) -> None:
        self._stop.set()

    # ---- shard discovery ------------------------------------------------

    async def _list_shards(self, kinesis: Any) -> list[str]:
        shards: list[str] = []
        next_token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"StreamName": self._stream_name, "MaxResults": 1000}
            if next_token:
                kwargs = {"NextToken": next_token}
            resp = await kinesis.list_shards(**kwargs)
            for s in resp.get("Shards", []):
                shards.append(s["ShardId"])
            next_token = resp.get("NextToken")
            if not next_token:
                break
        if not shards:
            raise ReplicatorError(f"stream {self._stream_name} has no shards")
        return shards

    # ---- per-shard worker -----------------------------------------------

    async def _shard_worker(self, kinesis: Any, shard_id: str) -> None:
        shard_key = f"{self._stream_name}:{shard_id}"

        # M4: steal the lease before doing anything. If someone else owns it, skip.
        lease_token = await self._try_lease(shard_id)
        if lease_token is None:
            log.info("shard.lease.skipped", shard=shard_id, task_id=self._task_id)
            return
        self.leases_acquired += 1

        # M4: resume from stored checkpoint if any; else LATEST (or TRIM_HORIZON if
        # the operator asked for a full backfill via start_from_horizon).
        iterator, iterator_type, starting_sequence = await self._resolve_iterator(kinesis, shard_id)
        log.info(
            "shard.start",
            shard=shard_id,
            iterator_type=iterator_type,
            starting_sequence=starting_sequence,
            task_id=self._task_id,
        )

        self._ordering.register_shard(shard_key)
        fragment = FragmentReassembler(timeout_seconds=self._cfg.fragment_timeout_ms / 1000.0)
        # NB: transaction reassembly is process-global on `self._txn` — see __init__ for the why.
        latest_ts_ns = 0
        latest_sequence_number: str | None = None
        last_checkpoint_flush_mono = time.monotonic()
        lease_lost = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(shard_id, lease_token, lease_lost),
            name=f"heartbeat:{shard_key}",
        )

        while not self._stop.is_set() and iterator is not None and not lease_lost.is_set():
            try:
                resp = await kinesis.get_records(ShardIterator=iterator, Limit=1000)
            except Exception as exc:
                log.warning("shard.get_records.error", shard=shard_id, error=str(exc))
                await asyncio.sleep(1.0)
                continue
            iterator = resp.get("NextShardIterator")
            mbl = int(resp.get("MillisBehindLatest", 0))
            records = resp.get("Records", [])
            self.records_received += len(records)
            self._shard_state[shard_id] = {
                "millis_behind_latest": mbl,
                "latest_ts_ns": latest_ts_ns,
                "lease_token": lease_token,
                "last_poll_mono": time.monotonic(),
            }

            for rec in records:
                data = rec["Data"]
                seq = rec.get("SequenceNumber")
                if seq is not None:
                    latest_sequence_number = str(seq)
                try:
                    env = parse(data)
                except EnvelopeError as exc:
                    self.parse_errors += 1
                    log.warning("parse.failed", shard=shard_id, error=str(exc), head=data[:120])
                    continue

                try:
                    ready = fragment.ingest(env)
                except CrcMismatchError as exc:
                    self.parse_errors += 1
                    log.warning("fragment.crc_mismatch", shard=shard_id, error=str(exc))
                    continue
                if ready is None:
                    continue

                completed = self._txn.ingest(ready)
                if completed is None:
                    # Still waiting on other records in this txn — but we can tighten the
                    # per-shard watermark from the records we have observed so far.
                    ts = _ts_ns_of_ready(ready)
                    if ts > latest_ts_ns:
                        latest_ts_ns = ts
                    continue

                if completed.ts_ns > latest_ts_ns:
                    latest_ts_ns = completed.ts_ns

                while True:
                    try:
                        self._ordering.ingest(completed)
                        break
                    except BackpressureError:
                        if self._stop.is_set():
                            break
                        await asyncio.sleep(0.1)

            # Even on empty batches, keep the watermark fresh so idle shards can advance.
            self._ordering.update_shard(shard_key, latest_ts_ns=latest_ts_ns, millis_behind_latest=mbl)

            # Sweep per-shard fragment timeouts. Transaction-level sweep is global —
            # see `_txn_sweeper` — because the reassembler is shared across shards.
            for ft in fragment.sweep():
                log.warning("fragment.timeout", shard=shard_id, chunk_id=ft.chunk_id)

            # M4: periodic (best-effort) checkpoint flush. Replay-on-restart is
            # protected by the `__replicator_applied` stamp table.
            now_mono = time.monotonic()
            if (
                latest_sequence_number is not None
                and (now_mono - last_checkpoint_flush_mono) >= self._checkpoint_flush_s
            ):
                await self._flush_checkpoint(shard_id, latest_sequence_number, latest_ts_ns)
                last_checkpoint_flush_mono = now_mono

            if not records:
                await asyncio.sleep(0.25)

        log.info("shard.stop", shard=shard_id, lease_lost=lease_lost.is_set())
        self._ordering.forget_shard(shard_key)

        # Flush final checkpoint and release the lease.
        if latest_sequence_number is not None:
            await self._flush_checkpoint(shard_id, latest_sequence_number, latest_ts_ns)
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await heartbeat_task
        if not lease_lost.is_set():
            await self._release_lease(shard_id, lease_token)

    # ---- apply runner ---------------------------------------------------

    async def _apply_runner(self) -> None:
        """Accumulate ready txns into size-or-time batches, dispatch as concurrent
        batch-apply tasks (bounded by `applier_workers`).

        Micro-batching (A) packs up to `batch_max_txns` source txns per cross-region
        BEGIN..COMMIT; partial batches flush after `batch_max_wait_ms`. Per-key
        ordering is preserved: within a batch, DMLs run in ts_ns order inside one
        SQL txn; across batches, stripe locks serialize touching the same key.

        On a non-retryable batch failure, the task falls back to single-txn apply
        per `self._apply_batch_task`, so fence/DLQ granularity stays per-source-txn.
        """
        log.info(
            "apply_runner.start",
            stripes=self._cfg.applier_stripes,
            workers=self._cfg.applier_workers,
            batch_max_txns=self._cfg.batch_max_txns,
            batch_max_wait_ms=self._cfg.batch_max_wait_ms,
        )
        sem = asyncio.Semaphore(self._cfg.applier_workers)
        in_flight: set[asyncio.Task[None]] = set()
        fenced = asyncio.Event()
        pending: list[Any] = []  # list[CompletedTransaction]
        pending_first_mono = 0.0
        batch_max = max(1, self._cfg.batch_max_txns)
        batch_wait_s = max(0.0, self._cfg.batch_max_wait_ms / 1000.0)

        async def _dispatch() -> None:
            nonlocal pending, pending_first_mono
            if not pending:
                return
            batch = pending
            pending = []
            pending_first_mono = 0.0
            await sem.acquire()
            task = asyncio.create_task(
                self._apply_batch_task(batch, sem, fenced),
                name=f"apply_batch:n={len(batch)}",
            )
            in_flight.add(task)
            task.add_done_callback(in_flight.discard)

        while not self._stop.is_set():
            # If a fence has been opened, drain in-flight (and any pending batch) and
            # wait for the fence poller to clear it.
            if fenced.is_set():
                if pending:
                    # Re-ingest so the ordering buffer can regate under the fence.
                    for t in pending:
                        with contextlib.suppress(BackpressureError):
                            self._ordering.ingest(t)
                    pending = []
                    pending_first_mono = 0.0
                if in_flight:
                    await asyncio.wait(in_flight, return_when=asyncio.ALL_COMPLETED)
                    in_flight.clear()
                await asyncio.sleep(0.05)
                if self._ordering.fence_ts_ns is None:
                    fenced.clear()
                continue

            ready = self._ordering.ready()
            for completed in ready:
                if self._stop.is_set() or fenced.is_set():
                    with contextlib.suppress(BackpressureError):
                        self._ordering.ingest(completed)
                    continue
                if not pending:
                    pending_first_mono = time.monotonic()
                pending.append(completed)
                if len(pending) >= batch_max:
                    await _dispatch()

            # Time-based flush of a partial batch.
            if pending and (time.monotonic() - pending_first_mono) >= batch_wait_s:
                await _dispatch()

            if not ready and not pending:
                await asyncio.sleep(0.05)

        # Clean shutdown: flush any pending partial batch, then await in-flight.
        if pending:
            await _dispatch()
        if in_flight:
            await asyncio.wait(in_flight, return_when=asyncio.ALL_COMPLETED)
        log.info("apply_runner.stop")

    async def _apply_batch_task(
        self,
        batch: list[Any],
        sem: asyncio.Semaphore,
        fenced: asyncio.Event,
    ) -> None:
        """One concurrent batch-apply task.

        Happy path: `Applier.apply_batch(batch)` commits all N source txns in one
        cross-region transaction. Each result still carries per-txn rows_applied /
        rows_deduped for metrics.

        Failure path: on `ApplyError`, fall back to per-txn `Applier.apply()`. The
        bad txn opens a fence at its own ts_ns (unchanged DLQ semantics); earlier
        txns in the batch still commit; later txns are re-ingested into the
        ordering buffer so they re-release past the fence.
        """
        try:
            try:
                results = await self._applier.apply_batch(batch)
                for completed, result in zip(batch, results, strict=True):
                    self.transactions_committed += 1
                    self.rows_replicated += result.rows_applied
                    self.rows_deduped += result.rows_deduped
                    self.rows_late_discarded += result.rows_late_discarded
                    log.info(
                        "apply.ok",
                        tx_id=completed.tx_id,
                        rows=result.rows_applied,
                        dedup=result.rows_deduped,
                        attempt=result.attempt,
                        duration_ms=round(result.duration_ms, 1),
                        stripes=list(result.stripes),
                        batched=len(batch),
                    )
                return
            except ApplyError as batch_exc:
                log.warning(
                    "apply_batch.failed_falling_back",
                    size=len(batch),
                    error=str(batch_exc),
                )
            # Per-txn fallback. One commit per txn so the bad txn gets its own fence.
            for i, completed in enumerate(batch):
                try:
                    result = await self._applier.apply(completed)
                    self.transactions_committed += 1
                    self.rows_replicated += result.rows_applied
                    self.rows_deduped += result.rows_deduped
                    self.rows_late_discarded += result.rows_late_discarded
                    log.info(
                        "apply.ok",
                        tx_id=completed.tx_id,
                        rows=result.rows_applied,
                        dedup=result.rows_deduped,
                        attempt=result.attempt,
                        duration_ms=round(result.duration_ms, 1),
                        stripes=list(result.stripes),
                        single_txn_fallback=True,
                    )
                except ApplyError as inner:
                    self.apply_errors += 1
                    log.error(
                        "apply.failed",
                        tx_id=completed.tx_id,
                        ts_ns=completed.ts_ns,
                        error=str(inner),
                    )
                    fenced.set()
                    await self._open_fence(completed, inner)
                    # Hand remaining txns back to the ordering buffer so they
                    # re-release once the fence closes.
                    for r in batch[i + 1 :]:
                        with contextlib.suppress(BackpressureError):
                            self._ordering.ingest(r)
                    return
        finally:
            sem.release()

    async def _apply_one(
        self,
        completed: Any,
        sem: asyncio.Semaphore,
        fenced: asyncio.Event,
    ) -> None:
        """One concurrent apply task. Releases the semaphore + opens fence on error."""
        try:
            result = await self._applier.apply(completed)
            self.transactions_committed += 1
            self.rows_replicated += result.rows_applied
            self.rows_deduped += result.rows_deduped
            self.rows_late_discarded += result.rows_late_discarded
            log.info(
                "apply.ok",
                tx_id=completed.tx_id,
                rows=result.rows_applied,
                dedup=result.rows_deduped,
                late_discarded=result.rows_late_discarded,
                attempt=result.attempt,
                duration_ms=round(result.duration_ms, 1),
                stripes=list(result.stripes),
            )
        except ApplyError as exc:
            self.apply_errors += 1
            log.error(
                "apply.failed",
                tx_id=completed.tx_id,
                ts_ns=completed.ts_ns,
                error=str(exc),
            )
            fenced.set()
            await self._open_fence(completed, exc)
        finally:
            sem.release()

    async def _open_fence(self, completed: Any, exc: BaseException) -> None:
        try:
            await self._dlq.open_fence(
                completed,
                error_class=type(exc).__name__,
                failing_sql=str(exc),
            )
            self._ordering.set_fence(completed.ts_ns)
            self.fences_opened += 1
        except Exception as fence_exc:
            log.error("fence.open.failed", tx_id=completed.tx_id, error=str(fence_exc))
            # Fallback: without a durable fence we must halt to avoid silent divergence.
            self._stop.set()

    # ---- leases + checkpoints -------------------------------------------

    async def _try_lease(self, shard_id: str) -> UUID | None:
        async def body(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> UUID | None:
            return await LeaseRepo(conn).steal(
                stream_name=self._stream_name,
                shard_id=shard_id,
                task_id=self._task_id,
                ttl_seconds=self._lease_ttl_s,
            )

        result = await self._state_call(body)
        return result if result is None else UUID(str(result))

    async def _release_lease(self, shard_id: str, lease_token: UUID) -> None:
        async def body(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
            await LeaseRepo(conn).release(
                stream_name=self._stream_name,
                shard_id=shard_id,
                task_id=self._task_id,
                lease_token=lease_token,
            )

        await self._state_call(body)

    async def _heartbeat_loop(self, shard_id: str, lease_token: UUID, lease_lost: asyncio.Event) -> None:
        try:
            while not self._stop.is_set() and not lease_lost.is_set():
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=self._lease_heartbeat_s)
                if self._stop.is_set() or lease_lost.is_set():
                    return

                async def body(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> bool:
                    return await LeaseRepo(conn).heartbeat(
                        stream_name=self._stream_name,
                        shard_id=shard_id,
                        task_id=self._task_id,
                        lease_token=lease_token,
                        ttl_seconds=self._lease_ttl_s,
                    )

                try:
                    ok = await self._state_call(body)
                except Exception as exc:
                    log.warning("shard.heartbeat.error", shard=shard_id, error=str(exc))
                    continue
                if not ok:
                    log.warning("shard.lease.lost", shard=shard_id, task_id=self._task_id)
                    self.leases_lost += 1
                    lease_lost.set()
                    return
        except asyncio.CancelledError:
            raise

    async def _resolve_iterator(self, kinesis: Any, shard_id: str) -> tuple[str | None, str, str | None]:
        """Return (iterator, iterator_type, starting_sequence_or_None)."""

        async def body(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> Any:
            return await CheckpointRepo(conn).get(self._stream_name, shard_id)

        cp = await self._state_call(body)
        if cp is not None:
            try:
                it_resp = await kinesis.get_shard_iterator(
                    StreamName=self._stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                    StartingSequenceNumber=cp.sequence_number,
                )
                return it_resp["ShardIterator"], "AFTER_SEQUENCE_NUMBER", cp.sequence_number
            except Exception as exc:
                log.warning(
                    "shard.resume.failed",
                    shard=shard_id,
                    sequence=cp.sequence_number,
                    error=str(exc),
                )
                # Fall through to LATEST / TRIM_HORIZON below.
        iterator_type = "TRIM_HORIZON" if self._start_from_horizon else "LATEST"
        it_resp = await kinesis.get_shard_iterator(
            StreamName=self._stream_name,
            ShardId=shard_id,
            ShardIteratorType=iterator_type,
        )
        return it_resp["ShardIterator"], iterator_type, None

    async def _flush_checkpoint(self, shard_id: str, sequence_number: str, latest_ts_ns: int) -> None:
        async def body(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
            async with conn.cursor() as cur:
                await CheckpointRepo(conn).upsert_in_cursor(
                    cur,
                    stream_name=self._stream_name,
                    shard_id=shard_id,
                    sequence_number=sequence_number,
                    last_applied_ts_ns=latest_ts_ns,
                )

        try:
            await self._state_call(body)
            self.checkpoints_flushed += 1
        except Exception as exc:
            log.warning("shard.checkpoint.flush_failed", shard=shard_id, error=str(exc))

    # ---- state-connection lifecycle ------------------------------------

    async def _state_call(self, func: Any) -> Any:
        """Run ``await func(conn)`` under the state lock, reconnecting + retrying once
        on a closed/broken connection. Fixes the "connection closed" wedge where a
        token-expired state connection would spin forever (M6.4 post-500TPS finding)."""
        async with self._state_lock:
            for attempt in range(2):
                try:
                    conn = await self._borrow_state_conn()
                    return await func(conn)
                except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                    if attempt == 0:
                        log.warning("state_conn.reconnect", error=str(exc))
                        await self._refresh_state_conn()
                        continue
                    raise
        raise RuntimeError("unreachable: _state_call exhausted retries without raising")

    async def _borrow_state_conn(self) -> psycopg.AsyncConnection[tuple[object, ...]]:
        if self._state_conn is None or self._state_conn.closed:
            await self._refresh_state_conn()
        assert self._state_conn is not None
        return self._state_conn

    async def _refresh_state_conn(self) -> None:
        """Close any existing state conn and open a fresh one with a refreshed token."""
        old = self._state_conn
        self._state_conn = None
        if old is not None and not old.closed:
            with contextlib.suppress(Exception):
                await old.close()
        await self._state_token_cache.get_async(force_refresh=True)
        self._state_conn = await open_connection(self._target, self._state_token_cache, autocommit=True)
        log.info("state_conn.refreshed")

    # ---- metrics loop ---------------------------------------------------

    async def _metrics_loop(self) -> None:
        """Every `METRICS_INTERVAL_MS`, snapshot counters/gauges and emit to CloudWatch.

        Counters are emitted as deltas since last tick so dashboard `Sum` statistic shows
        rate-per-window naturally. Gauges emit current values (CloudWatch `Average` works).
        """
        log.info("metrics_loop.start", interval_s=self._metrics_interval_s)
        while not self._stop.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self._metrics_interval_s)
            if self._stop.is_set():
                break
            try:
                await self._emit_metrics()
                _touch_liveness()
            except Exception as exc:
                log.warning("metrics_loop.error", error=str(exc))
        log.info("metrics_loop.stop")

    async def _emit_metrics(self) -> None:
        stream_dim: tuple[tuple[str, str], ...] = (("Stream", self._stream_name),)
        points: list[MetricPoint] = []

        # Counter deltas since last tick.
        counter_names = (
            "records_received",
            "transactions_committed",
            "rows_replicated",
            "rows_deduped",
            "rows_late_discarded",
            "parse_errors",
            "apply_errors",
            "fences_opened",
            "fences_closed_observed",
            "leases_acquired",
            "leases_lost",
            "checkpoints_flushed",
            "txn_timeouts",
        )
        for name in counter_names:
            cur_val = int(getattr(self, name))
            prev = self._prev_counters.get(name, 0)
            delta = max(0, cur_val - prev)
            self._prev_counters[name] = cur_val
            points.append(MetricPoint(_camel(name), float(delta), "Count", stream_dim))

        # Gauges from ordering buffer.
        stats = self._ordering.stats()
        now_ns = time.time_ns()
        points.append(MetricPoint("OrderingBufferDepth", float(stats.depth), "Count", stream_dim))
        if stats.watermark_ns is not None:
            lag_ms = max(0.0, (now_ns - stats.watermark_ns) / 1e6)
            points.append(MetricPoint("WatermarkLagMs", lag_ms, "Milliseconds", stream_dim))
            points.append(MetricPoint("WatermarkTsNs", float(stats.watermark_ns), "None", stream_dim))
        if stats.head_ts_ns is not None:
            head_age_ms = max(0.0, (now_ns - stats.head_ts_ns) / 1e6)
            points.append(MetricPoint("OrderingBufferHeadAgeMs", head_age_ms, "Milliseconds", stream_dim))
        points.append(
            MetricPoint(
                "IdleShardSyntheticAdvances",
                float(stats.idle_shards_synthetically_advanced),
                "Count",
                stream_dim,
            )
        )
        points.append(
            MetricPoint(
                "FenceActive",
                1.0 if stats.fence_ts_ns is not None else 0.0,
                "None",
                stream_dim,
            )
        )

        # Per-shard gauges.
        for shard_id, state in self._shard_state.items():
            shard_dim = (*stream_dim, ("Shard", shard_id))
            points.append(
                MetricPoint(
                    "KinesisMillisBehindLatest",
                    float(state.get("millis_behind_latest", 0)),
                    "Milliseconds",
                    shard_dim,
                )
            )

        # Open fence age (one query per emit; low cost).
        try:
            fence_row = await self._dlq.fetch_open_fence()
            if fence_row is not None:
                age_sec = max(
                    0.0,
                    (datetime.now(fence_row.opened_at.tzinfo) - fence_row.opened_at).total_seconds(),
                )
                points.append(MetricPoint("OpenFenceCount", 1.0, "Count", stream_dim))
                points.append(MetricPoint("OpenFenceAgeSec", age_sec, "Seconds", stream_dim))
            else:
                points.append(MetricPoint("OpenFenceCount", 0.0, "Count", stream_dim))
                points.append(MetricPoint("OpenFenceAgeSec", 0.0, "Seconds", stream_dim))
        except Exception as exc:
            log.debug("metrics.fence_probe.error", error=str(exc))

        await self._metrics.put_batch(points)

    # ---- transaction reassembly sweeper ---------------------------------

    async def _txn_sweeper(self) -> None:
        """Global sweep of the shared TransactionReassembler.

        Shard workers push records into `self._txn` but never sweep it — a per-shard sweep
        would drop cross-shard transactions prematurely. This single-process task is the
        only place `txn.timeout` is observed, and increments `self.txn_timeouts` for the
        `TxnTimeouts` CloudWatch metric so this failure mode is never silent.
        """
        log.info("txn_sweeper.start", interval_s=self._txn_sweep_interval_s)
        while not self._stop.is_set():
            try:
                for tt in self._txn.sweep():
                    self.txn_timeouts += 1
                    log.warning(
                        "txn.timeout",
                        tx_id=tt.tx_id,
                        buffered=tt.buffered,
                        expected=tt.expected,
                    )
            except Exception as exc:
                log.warning("txn_sweeper.error", error=str(exc))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._txn_sweep_interval_s)
            except TimeoutError:
                continue
        log.info("txn_sweeper.stop")

    # ---- fence poller ---------------------------------------------------

    async def _fence_poller(self) -> None:
        """Watch target DSQL for fence-close signals from the ops CLI.

        When no OPEN fence row remains, clear the in-memory fence so the ordering buffer
        resumes draining in `ts_ns` order. New apply failures restart this loop by
        reopening a fence row.
        """
        log.info("fence_poller.start", interval_s=self._fence_poll_interval_s)
        while not self._stop.is_set():
            try:
                row = await self._dlq.fetch_open_fence()
                in_memory = self._ordering.fence_ts_ns
                if row is None and in_memory is not None:
                    self._ordering.set_fence(None)
                    self.fences_closed_observed += 1
                    log.info("fence_poller.cleared", prev_fence_ts_ns=in_memory)
                elif row is not None and in_memory != row.fence_ts_ns:
                    # Operator or another worker opened a fence we didn't see yet;
                    # mirror it so we don't release past it.
                    self._ordering.set_fence(row.fence_ts_ns)
                    log.info("fence_poller.synced", fence_ts_ns=row.fence_ts_ns, tx_id=row.tx_id)
            except Exception as exc:
                log.warning("fence_poller.error", error=str(exc))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._fence_poll_interval_s)
            except TimeoutError:
                continue
        log.info("fence_poller.stop")

    # ---- signals --------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, ValueError):
                loop.add_signal_handler(sig, self.stop)


def _ts_ns_of_ready(ready: Any) -> int:
    """Pull `source.ts_ns` from a FullRecord or AssembledRecord."""
    main = getattr(ready, "main", None)
    source = getattr(main or ready, "source", None)
    return int(getattr(source, "ts_ns", 0)) if source is not None else 0


def _camel(snake: str) -> str:
    """`records_received` → `RecordsReceived` for CloudWatch metric names."""
    return "".join(part.capitalize() for part in snake.split("_"))


def _liveness_path() -> str:
    """Per-process liveness file when running under the multi-process supervisor
    (`PROCESS_ID` is set in each child), otherwise the single-process path.
    `health._probe_liveness` reads `REPLICATOR_PROCESSES` and checks each file."""
    import os as _os

    pid = _os.environ.get("PROCESS_ID")
    if pid is not None:
        return f"/tmp/dsql-replicator-liveness-{pid}"
    return "/tmp/dsql-replicator-liveness"


def _touch_liveness() -> None:
    """Update the liveness-heartbeat file mtime. Called from the metrics loop.

    The container HEALTHCHECK (`dsql-replicator healthz`) reads this file's age — a
    stale file means the pipeline is wedged even if all AWS probes still pass."""
    from pathlib import Path

    with contextlib.suppress(OSError):
        Path(_liveness_path()).touch(exist_ok=True)
