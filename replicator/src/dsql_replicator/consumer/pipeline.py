"""Row-level consumer pipeline (post-2026-11 simplification pass).

Per-shard flow:

    Kinesis GetRecords
        │  bytes
        v
    envelope.parse                   ── EnvelopeError → log + skip
        │  CdcRecord
        v
    FragmentReassembler.ingest       ── CrcMismatch / sweep timeouts → log + skip
        │  ReadyRecord (FullRecord | AssembledRecord) | None
        v
    apply queue (shared across shards)
        │
        v
    apply workers → Applier.apply_batch(records, pre_commit_hook=checkpoint)

Design decisions in this pass (see docs/DESIGN.md for the full contract):

- **No transaction reassembly / no ordering buffer / no DLQ fence.** Kinesis
  does not preserve DSQL transaction order across shards, so the old
  machinery delivered no real transactional guarantee. The applier LWW stamp
  provides per-primary-key eventual convergence; cross-row invariants held
  by a source transaction can be violated on the replica.

- **Apply retries forever.** No fence, no bad-payload quarantine — a
  persistent apply failure blocks only its own stripe locks and the pipeline
  keeps moving on other keys. Operator action is still required to fix a
  real target-side defect; CloudWatch alarms on `ApplyRetries` surface it.

- **Per-shard checkpoints are best-effort.** The shard worker periodically
  upserts `__replicator_checkpoints` to the latest consumed Kinesis
  sequence for its own shard. A crash can lose records that were parsed on
  one shard but not yet applied (e.g. fragments awaiting cross-shard
  reassembly); LWW makes re-apply idempotent on the next source write that
  touches the same primary key. Document in README under "Known limits".

Still present (kept intentionally):
  - Fragment reassembler (handles >1 MB rows that Aurora DSQL splits).
  - Writer-pool autoscaler driven by replication lag.
  - Shard leases (single writer per shard) + periodic shard rediscovery.
  - `__replicator_lww_stamps` table (non-invasive — customer tables untouched).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import time
from typing import TYPE_CHECKING, Any
from uuid import UUID

import aioboto3
import psycopg
import structlog

from dsql_replicator.applier import Applier, ApplyError
from dsql_replicator.applier.autoscaler import AutoscaleParams, WorkerAutoscaler
from dsql_replicator.applier.connection import DsqlEndpoint, DsqlTokenCache, open_connection
from dsql_replicator.config import Config
from dsql_replicator.envelope import parse
from dsql_replicator.errors import EnvelopeError, ReplicatorError
from dsql_replicator.metrics import MetricPoint, MetricsEmitter
from dsql_replicator.reassembler import CrcMismatchError, FragmentReassembler, ReadyRecord
from dsql_replicator.state import CheckpointRepo, LeaseRepo

if TYPE_CHECKING:
    pass

log = structlog.get_logger("consumer")


class _QueueItem:
    """A record waiting in the apply queue, tagged with its origin shard + seq."""

    __slots__ = ("enqueued_mono", "record", "seq_str", "shard_id")

    def __init__(self, record: ReadyRecord, shard_id: str, seq_str: str) -> None:
        self.record = record
        self.shard_id = shard_id
        self.seq_str = seq_str
        self.enqueued_mono = time.monotonic()


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
        self._applier = Applier(
            target_endpoint,
            backoff_initial_s=cfg.apply_backoff_initial_ms / 1000.0,
            stripe_count=cfg.applier_stripes,
            worker_count=cfg.applier_workers,
            max_worker_count=cfg.applier_workers_max or cfg.applier_workers,
        )
        # DSQL CDC distributes a single chunked record's fragments across every
        # open shard, so the fragment reassembler is process-global.
        self._fragment = FragmentReassembler(
            timeout_seconds=cfg.fragment_timeout_ms / 1000.0,
        )
        self._reassembly_sweep_interval_s = max(0.25, cfg.fragment_timeout_ms / 1000.0 / 4)
        self.stream_name = stream_name

        # Shared apply queue drained by apply workers. Bounded so a stall in
        # the apply pool exerts backpressure on the shard workers.
        self._apply_queue: asyncio.Queue[_QueueItem] = asyncio.Queue(
            maxsize=cfg.ordering_buffer_max_depth
        )

        # Per-shard checkpoint bookkeeping: the max consumed Kinesis seq seen
        # on each shard, plus the last value we persisted (so the flush task
        # only writes when it strictly advances).
        self._shard_max_consumed: dict[str, str] = {}
        self._shard_last_flushed: dict[str, str] = {}

        # State plane: own connection for CheckpointRepo + LeaseRepo.
        self._state_token_cache = DsqlTokenCache(target_endpoint)
        self._state_conn: psycopg.AsyncConnection[tuple[object, ...]] | None = None
        self._state_lock = asyncio.Lock()
        self._task_id = cfg.task_id
        self._lease_ttl_s = cfg.lease_ttl_seconds
        self._lease_heartbeat_s = cfg.lease_heartbeat_seconds
        self._checkpoint_flush_interval_s = max(0.25, cfg.checkpoint_flush_interval_ms / 1000.0)

        # Running counters surfaced to logs + CloudWatch.
        self.records_received = 0
        self.rows_applied = 0
        self.rows_discarded = 0
        self.parse_errors = 0
        self.crc_mismatches = 0
        self.fragment_timeouts = 0
        self.apply_batches = 0
        self.leases_acquired = 0
        self.leases_lost = 0
        self.checkpoints_flushed = 0

        # Autoscaler lag signal: wall-time age of the newest source ts_ns we
        # have observed applying. Updated by the apply loop.
        self._last_applied_source_ts_ns: int = 0

        # Metrics setup.
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
                worker_tasks: dict[str, asyncio.Task[None]] = {
                    s: asyncio.create_task(self._shard_worker(kinesis, s), name=f"shard:{s}")
                    for s in shards
                }
                apply_tasks = [
                    asyncio.create_task(self._apply_worker(i), name=f"apply:{i}")
                    for i in range(max(1, self._cfg.applier_workers))
                ]
                metrics_task = asyncio.create_task(self._metrics_loop(), name="metrics-loop")
                sweep_task = asyncio.create_task(self._reassembly_sweeper(), name="reassembly-sweeper")
                checkpoint_task = asyncio.create_task(
                    self._checkpoint_flush_loop(), name="checkpoint-flush"
                )
                discovery_task = asyncio.create_task(
                    self._shard_discovery_loop(kinesis, worker_tasks),
                    name="shard-discovery",
                )
                autoscaler = WorkerAutoscaler(
                    applier=self._applier,
                    lag_ms_signal=self._current_lag_ms,
                    params=AutoscaleParams(
                        floor=self._cfg.applier_workers,
                        ceiling=self._cfg.applier_workers_max or self._cfg.applier_workers,
                        step=self._cfg.applier_scale_step,
                        tick_s=self._cfg.applier_scale_tick_ms / 1000.0,
                        scale_up_lag_ms=float(self._cfg.applier_scale_up_lag_ms),
                        scale_up_sustain_s=self._cfg.applier_scale_up_sustain_ms / 1000.0,
                        scale_down_lag_ms=float(self._cfg.applier_scale_down_lag_ms),
                        scale_down_sustain_s=self._cfg.applier_scale_down_sustain_ms / 1000.0,
                    ),
                    stop_event=self._stop,
                )
                autoscaler_task = asyncio.create_task(autoscaler.run(), name="writer-autoscaler")
                try:
                    await self._stop.wait()
                finally:
                    for t in worker_tasks.values():
                        t.cancel()
                    for t in apply_tasks:
                        t.cancel()
                    metrics_task.cancel()
                    sweep_task.cancel()
                    checkpoint_task.cancel()
                    discovery_task.cancel()
                    autoscaler_task.cancel()
                    await asyncio.gather(
                        *worker_tasks.values(),
                        *apply_tasks,
                        metrics_task,
                        sweep_task,
                        checkpoint_task,
                        discovery_task,
                        autoscaler_task,
                        return_exceptions=True,
                    )
        finally:
            await self._applier.close()
            if self._state_conn is not None and not self._state_conn.closed:
                await self._state_conn.close()
            self._state_conn = None
            log.info(
                "consumer.stopped",
                records=self.records_received,
                rows=self.rows_applied,
                discarded=self.rows_discarded,
                parse_errors=self.parse_errors,
                crc_mismatches=self.crc_mismatches,
                fragment_timeouts=self.fragment_timeouts,
                apply_batches=self.apply_batches,
                leases_acquired=self.leases_acquired,
                leases_lost=self.leases_lost,
                checkpoints_flushed=self.checkpoints_flushed,
            )

    def stop(self) -> None:
        self._stop.set()

    def _current_lag_ms(self) -> float | None:
        """Writer autoscaler signal: wall-time age of the newest source
        ts_ns we've seen apply. None before the first apply."""
        if self._last_applied_source_ts_ns == 0:
            return None
        return max(0.0, (time.time_ns() - self._last_applied_source_ts_ns) / 1e6)

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

    async def _shard_discovery_loop(
        self, kinesis: Any, worker_tasks: dict[str, asyncio.Task[None]]
    ) -> None:
        """Periodically re-list Kinesis shards and spawn workers for new ones."""
        interval_s = self._cfg.shard_discovery_interval_ms / 1000.0
        if interval_s <= 0:
            log.info("shard.discovery.disabled", stream=self._stream_name)
            return
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval_s)
                return
            except TimeoutError:
                pass
            try:
                discovered = set(await self._list_shards(kinesis))
            except Exception as exc:
                log.warning(
                    "shard.discovery.list_failed",
                    stream=self._stream_name,
                    error=str(exc),
                )
                continue

            for sid in [s for s, t in worker_tasks.items() if t.done()]:
                worker_tasks.pop(sid, None)

            new_shards = discovered - worker_tasks.keys()
            if new_shards:
                log.info(
                    "shard.discovery.new",
                    stream=self._stream_name,
                    new_shards=sorted(new_shards),
                    live_shards=sorted(worker_tasks.keys()),
                )
                for sid in sorted(new_shards):
                    worker_tasks[sid] = asyncio.create_task(
                        self._shard_worker(kinesis, sid), name=f"shard:{sid}"
                    )

    # ---- per-shard worker -----------------------------------------------

    async def _shard_worker(self, kinesis: Any, shard_id: str) -> None:
        shard_key = f"{self._stream_name}:{shard_id}"
        _ = shard_key  # dimensional tag reserved for future per-shard metrics

        lease_token = await self._try_lease(shard_id)
        if lease_token is None:
            log.info("shard.lease.skipped", shard=shard_id, task_id=self._task_id)
            return
        self.leases_acquired += 1

        iterator, iterator_type, starting_sequence = await self._resolve_iterator(kinesis, shard_id)
        log.info(
            "shard.start",
            shard=shard_id,
            iterator_type=iterator_type,
            starting_sequence=starting_sequence,
            task_id=self._task_id,
        )

        lease_lost = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(shard_id, lease_token, lease_lost),
            name=f"heartbeat:{shard_id}",
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
                "lease_token": lease_token,
                "last_poll_mono": time.monotonic(),
            }

            for rec in records:
                data = rec["Data"]
                seq = rec.get("SequenceNumber")
                seq_str = str(seq) if seq is not None else ""
                if seq_str:
                    self._shard_max_consumed[shard_id] = seq_str

                try:
                    env = parse(data)
                except EnvelopeError as exc:
                    self.parse_errors += 1
                    log.warning("parse.failed", shard=shard_id, error=str(exc), head=data[:120])
                    continue

                try:
                    ready = self._fragment.ingest(env)
                except CrcMismatchError as exc:
                    self.crc_mismatches += 1
                    log.warning("fragment.crc_mismatch", shard=shard_id, error=str(exc))
                    continue
                if ready is None:
                    continue

                await self._apply_queue.put(_QueueItem(ready, shard_id, seq_str))

            if not records:
                await asyncio.sleep(0.25)

        log.info("shard.stop", shard=shard_id, lease_lost=lease_lost.is_set())
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await heartbeat_task
        if not lease_lost.is_set():
            await self._release_lease(shard_id, lease_token)

    # ---- apply workers --------------------------------------------------

    async def _apply_worker(self, worker_id: int) -> None:
        """Drain the apply queue in micro-batches and hand them to the Applier.

        A worker collects up to `batch_max_txns` queue items or waits
        `batch_max_wait_ms` for a partial flush. Backpressure comes from the
        queue being empty (worker sleeps) or full (shard workers block on put).
        """
        _ = worker_id  # reserved for future per-worker logging
        batch_max = max(1, self._cfg.batch_max_txns)
        batch_wait_s = max(0.0, self._cfg.batch_max_wait_ms / 1000.0)

        while not self._stop.is_set():
            try:
                first = await asyncio.wait_for(self._apply_queue.get(), timeout=0.5)
            except TimeoutError:
                continue
            batch = [first]
            batch_deadline = time.monotonic() + batch_wait_s
            while len(batch) < batch_max:
                remaining = batch_deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    nxt = await asyncio.wait_for(self._apply_queue.get(), timeout=remaining)
                except TimeoutError:
                    break
                batch.append(nxt)

            await self._apply_one_batch(batch)

    async def _apply_one_batch(self, batch: list[_QueueItem]) -> None:
        hook = self._make_checkpoint_hook(batch)
        records = [it.record for it in batch]
        try:
            result = await self._applier.apply_batch(records, pre_commit_hook=hook)
        except ApplyError as exc:
            # Non-retryable (schema missing / malformed). Infinite retry
            # would spin forever on a structural problem; log and drop so
            # other keys keep flowing. Operator sees ApplyErrors in metrics.
            log.error(
                "apply.structural_failure_dropping",
                size=len(batch),
                error=str(exc),
            )
            # Still mark the shards' seqs so progress isn't permanently stuck.
            for it in batch:
                self._shard_max_consumed[it.shard_id] = max(
                    self._shard_max_consumed.get(it.shard_id, ""),
                    it.seq_str,
                    key=_seq_sort_key,
                )
            return
        self.apply_batches += 1
        self.rows_applied += result.rows_applied
        self.rows_discarded += result.rows_discarded
        # Advance the lag signal to the newest source ts_ns in the batch.
        for it in batch:
            ts = _ts_ns_of(it.record)
            if ts > self._last_applied_source_ts_ns:
                self._last_applied_source_ts_ns = ts
        log.info(
            "apply.ok",
            size=len(batch),
            rows=result.rows_applied,
            discarded=result.rows_discarded,
            attempt=result.attempt,
            duration_ms=round(result.duration_ms, 1),
            stripes=list(result.stripes),
        )

    def _make_checkpoint_hook(self, batch: list[_QueueItem]) -> Any:
        """Build an applier pre_commit_hook that upserts `__replicator_checkpoints`
        for every shard that contributed a record to this batch, using the
        same cursor the applier is about to COMMIT."""
        # Collect the max seq per shard in this batch.
        per_shard_max: dict[str, str] = {}
        max_ts_ns = 0
        for it in batch:
            if not it.seq_str or not it.shard_id:
                continue
            cur = per_shard_max.get(it.shard_id, "")
            per_shard_max[it.shard_id] = (
                it.seq_str if _seq_sort_key(it.seq_str) > _seq_sort_key(cur) else cur
            )
            ts = _ts_ns_of(it.record)
            if ts > max_ts_ns:
                max_ts_ns = ts

        async def hook(cur: Any) -> None:
            if not per_shard_max:
                return
            repo = CheckpointRepo(cur.connection)
            for shard_id, seq_str in per_shard_max.items():
                await repo.upsert_in_cursor(
                    cur,
                    stream_name=self._stream_name,
                    shard_id=shard_id,
                    sequence_number=seq_str,
                    last_applied_ts_ns=max_ts_ns,
                )
                self.checkpoints_flushed += 1

        return hook

    # ---- periodic checkpoint flush (shards with no committed records) --

    async def _checkpoint_flush_loop(self) -> None:
        """Periodically upsert the per-shard checkpoint even for shards that
        did not contribute an applied record recently.

        The in-band hook in `_make_checkpoint_hook` already advances a shard's
        checkpoint whenever one of its records applies. This loop catches
        shards whose records are stuck in fragment reassembly (and therefore
        never reach the apply hook) so a quiet shard's checkpoint still
        progresses toward its consumed seq. Row-level LWW makes the resulting
        re-fetch on restart idempotent."""
        log.info("checkpoint_flush.start", interval_s=self._checkpoint_flush_interval_s)
        while not self._stop.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self._checkpoint_flush_interval_s)
            if self._stop.is_set():
                break
            try:
                await self._flush_pending_checkpoints()
            except Exception as exc:
                log.warning("checkpoint_flush.error", error=str(exc))
        log.info("checkpoint_flush.stop")

    async def _flush_pending_checkpoints(self) -> None:
        to_flush: list[tuple[str, str]] = []
        for shard_id, seq_str in list(self._shard_max_consumed.items()):
            prev = self._shard_last_flushed.get(shard_id, "")
            if _seq_sort_key(seq_str) > _seq_sort_key(prev):
                to_flush.append((shard_id, seq_str))
        if not to_flush:
            return

        async def body(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
            repo = CheckpointRepo(conn)
            async with conn.cursor() as cur:
                for shard_id, seq_str in to_flush:
                    await repo.upsert_in_cursor(
                        cur,
                        stream_name=self._stream_name,
                        shard_id=shard_id,
                        sequence_number=seq_str,
                        last_applied_ts_ns=self._last_applied_source_ts_ns,
                    )

        await self._state_call(body)
        for shard_id, seq_str in to_flush:
            self._shard_last_flushed[shard_id] = seq_str
            self.checkpoints_flushed += 1

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
        iterator_type = "TRIM_HORIZON" if self._start_from_horizon else "LATEST"
        it_resp = await kinesis.get_shard_iterator(
            StreamName=self._stream_name,
            ShardId=shard_id,
            ShardIteratorType=iterator_type,
        )
        return it_resp["ShardIterator"], iterator_type, None

    # ---- state-connection lifecycle ------------------------------------

    async def _state_call(self, func: Any) -> Any:
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

        counter_names = (
            "records_received",
            "rows_applied",
            "rows_discarded",
            "parse_errors",
            "crc_mismatches",
            "fragment_timeouts",
            "apply_batches",
            "leases_acquired",
            "leases_lost",
            "checkpoints_flushed",
        )
        for name in counter_names:
            cur_val = int(getattr(self, name))
            prev = self._prev_counters.get(name, 0)
            delta = max(0, cur_val - prev)
            self._prev_counters[name] = cur_val
            points.append(MetricPoint(_camel(name), float(delta), "Count", stream_dim))

        # Replication lag gauge (same semantics as the autoscaler signal).
        lag = self._current_lag_ms()
        if lag is not None:
            points.append(MetricPoint("ReplicationLagMs", lag, "Milliseconds", stream_dim))

        points.append(
            MetricPoint("ApplyQueueDepth", float(self._apply_queue.qsize()), "Count", stream_dim)
        )

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

        await self._metrics.put_batch(points)

    # ---- fragment-reassembly sweeper ------------------------------------

    async def _reassembly_sweeper(self) -> None:
        """Global sweep of the shared FragmentReassembler.

        Chunked records' fragments can arrive on different shards, so this
        sweep must be process-global — a per-shard sweep would drop
        cross-shard fragment buffers prematurely."""
        log.info("reassembly_sweeper.start", interval_s=self._reassembly_sweep_interval_s)
        while not self._stop.is_set():
            try:
                for ft in self._fragment.sweep():
                    self.fragment_timeouts += 1
                    log.warning(
                        "fragment.timeout",
                        chunk_id=ft.chunk_id,
                        tx_id=ft.tx_id,
                        record_id=ft.record_id,
                        reason=ft.reason,
                    )
            except Exception as exc:
                log.warning("reassembly_sweeper.error", error=str(exc))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._reassembly_sweep_interval_s)
            except TimeoutError:
                continue
        log.info("reassembly_sweeper.stop")

    # ---- signals --------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, ValueError):
                loop.add_signal_handler(sig, self.stop)


# ---------- helpers ----------


def _ts_ns_of(rec: ReadyRecord) -> int:
    main = getattr(rec, "main", None)
    source = getattr(main or rec, "source", None)
    return int(getattr(source, "ts_ns", 0)) if source is not None else 0


def _seq_sort_key(seq: str) -> int:
    """Kinesis sequence numbers are decimal strings; compare numerically.

    A fallback hash keeps best-effort ordering for non-numeric test sequences
    without raising."""
    if not seq:
        return -1
    try:
        return int(seq)
    except (TypeError, ValueError):
        return hash(seq)


def _camel(snake: str) -> str:
    return "".join(part.capitalize() for part in snake.split("_"))


def _liveness_path() -> str:
    import os as _os

    pid = _os.environ.get("PROCESS_ID")
    if pid is not None:
        return f"/tmp/dsql-replicator-liveness-{pid}"
    return "/tmp/dsql-replicator-liveness"


def _touch_liveness() -> None:
    from pathlib import Path

    with contextlib.suppress(OSError):
        Path(_liveness_path()).touch(exist_ok=True)
