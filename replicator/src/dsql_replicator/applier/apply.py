"""Applier: write `CompletedTransaction`s to the target DSQL cluster atomically.

Design (M6.0 — per-key parallel apply, reversing the 2026-10-08 serial-lane decision):

- **Stripe locks.** The applier owns `stripe_count` asyncio.Locks (default 64). Each
  touched key `(schema, table, pk-tuple)` is hashed into a stripe id. To commit a txn,
  the worker acquires the sorted set of touched stripes (in sorted order to avoid
  deadlock), executes `BEGIN..COMMIT`, releases.

- **Connection pool.** `worker_count` dedicated psycopg3 connections (default 32) are
  held in an asyncio.Queue. Workers get a connection for the duration of one apply and
  return it. A separate schema connection services the SchemaCache for stripe
  computation.

- **Atomic row + stamp.** Each apply still writes the row(s) and the
  `__replicator_applied(tx_id, record_id)` stamp in ONE DSQL transaction.

- **UPSERT / DELETE SQL.** Unchanged from M1.

- **Retries.** Transient failures retry up to `APPLY_MAX_RETRIES` with exponential
  backoff + full jitter. Non-retryable failures raise `ApplyError` (caller opens fence).

Setting `stripe_count=1` + `worker_count=1` is backward-compatible with the serial
Applier (acts as one lock, one connection).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import random
import time
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any

import psycopg
import structlog
from psycopg import errors as pg_errors
from psycopg import sql

from dsql_replicator.dlq.schema import bootstrap_dlq_tables
from dsql_replicator.envelope import ChunkedRecord, FullRecord
from dsql_replicator.errors import ReplicatorError
from dsql_replicator.reassembler import AssembledRecord, CompletedTransaction, ReadyRecord
from dsql_replicator.state.schema import bootstrap_state_tables

from .coerce import coerce_value
from .connection import DsqlEndpoint, DsqlTokenCache, open_connection
from .schema import SchemaCache, SchemaNotFoundError, TableMeta

CursorHook = Callable[[psycopg.AsyncCursor[Any]], Awaitable[None]]

log = structlog.get_logger("applier")

_APPLIED_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS __replicator_applied (
    tx_id      text NOT NULL,
    record_id  text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tx_id, record_id)
)
"""


class ApplyError(ReplicatorError):
    """Permanent failure applying a transaction to the target. Caller should DLQ + fence."""


class RetryableApplyError(ReplicatorError):
    """Transient failure; the Applier retries internally and only raises `ApplyError` after
    `APPLY_MAX_RETRIES` is exhausted. Exposed for test introspection."""


class InjectedFaultError(RuntimeError):
    """Raised by the Applier's test-only fault hook. Simulates a crash mid-apply before
    COMMIT, forcing the `conn.transaction()` context manager to issue a ROLLBACK.
    See `Applier(_fault_after_rows=N)`."""


@dataclass(frozen=True)
class ApplyResult:
    tx_id: str
    rows_applied: int
    rows_deduped: int
    attempt: int
    duration_ms: float
    stripes: tuple[int, ...] = ()


# Transient classes that warrant a retry.
_RETRYABLE: tuple[type[BaseException], ...] = (
    psycopg.OperationalError,
    psycopg.InterfaceError,
    pg_errors.DeadlockDetected,
    pg_errors.SerializationFailure,
    ConnectionError,
    TimeoutError,
)


class Applier:
    """Per-key parallel-apply pipeline against one target DSQL cluster."""

    def __init__(
        self,
        endpoint: DsqlEndpoint,
        *,
        max_retries: int = 5,
        backoff_initial_s: float = 0.1,
        backoff_cap_s: float = 10.0,
        stripe_count: int = 64,
        worker_count: int = 32,
        _fault_after_rows: int | None = None,
    ) -> None:
        self._endpoint = endpoint
        self._token_cache = DsqlTokenCache(endpoint)
        self._max_retries = max_retries
        self._backoff_initial = backoff_initial_s
        self._backoff_cap = backoff_cap_s
        self._stripe_count = max(1, stripe_count)
        self._worker_count = max(1, worker_count)
        self._stripes: list[asyncio.Lock] = [asyncio.Lock() for _ in range(self._stripe_count)]
        self._conn_pool: asyncio.Queue[psycopg.AsyncConnection[tuple[object, ...]]] = asyncio.Queue(
            maxsize=self._worker_count
        )
        # Track each pool connection's open time so we can proactively recycle
        # before the IAM token (TTL ~15 min) expires server-side. Without this,
        # the first apply after a 15 min idle period hits dead sockets on every
        # retry and fences (incident: 2026-10-26 pool-idle SSL-EOF).
        self._conn_opened_mono: dict[int, float] = {}
        self._conn_max_age_s: float = 12 * 60.0  # recycle before 15-min token TTL
        self._schema_conn: psycopg.AsyncConnection[tuple[object, ...]] | None = None
        self._schema: SchemaCache | None = None
        self._bootstrapped = False
        # Test-only crash hook, see InjectedFaultError.
        self._fault_after_rows = _fault_after_rows

    # ---- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        """Open worker connections + schema connection; bootstrap state tables.

        Pool pre-warm is **staggered** (20 ms between connects) so N worker
        connections don't hit the DSQL endpoint simultaneously. Without the
        stagger, the 128-connection cold burst at task start combined with the
        unlocked token cache produced correlated `SSL EOF` failures and opened
        fences (incident: 2026-10-26). Total pre-warm time at 128 workers is
        ~2.5 s + per-connect latency, dwarfed by the first-ever DSQL warmup.
        """
        if self._schema_conn is not None:
            return
        self._schema_conn = await open_connection(self._endpoint, self._token_cache, autocommit=False)
        self._schema = SchemaCache(self._schema_conn)
        await self._bootstrap_tables()
        for i in range(self._worker_count):
            conn = await open_connection(self._endpoint, self._token_cache, autocommit=False)
            self._conn_opened_mono[id(conn)] = time.monotonic()
            await self._conn_pool.put(conn)
            # Only stagger non-trivial pool sizes; 1-worker test setups skip.
            if self._worker_count > 4 and i < self._worker_count - 1:
                await asyncio.sleep(0.02)

    async def close(self) -> None:
        while not self._conn_pool.empty():
            conn = self._conn_pool.get_nowait()
            if not conn.closed:
                await conn.close()
        self._conn_opened_mono.clear()
        if self._schema_conn is not None and not self._schema_conn.closed:
            await self._schema_conn.close()
        self._schema_conn = None

    async def _borrow_fresh_conn(self) -> psycopg.AsyncConnection[tuple[object, ...]]:
        """Pop a connection from the pool; if its token is near expiry, recycle it.

        DSQL IAM tokens live ~15 min. A pool connection opened at task start sits
        with an aging token every time the pool is quiet. Server-side the token
        expires and the next first-use read EOFs — which produced the 2026-10-26
        post-idle fence storm. We proactively close and reopen any conn whose
        open-age exceeds `_conn_max_age_s` (12 min, with a buffer under the TTL).
        """
        conn = await self._conn_pool.get()
        opened_at = self._conn_opened_mono.pop(id(conn), 0.0)
        if conn.closed or (time.monotonic() - opened_at) >= self._conn_max_age_s:
            # Replace a stale (or externally-closed) conn before handing it out.
            if not conn.closed:
                with contextlib.suppress(Exception):
                    await conn.close()
            try:
                await self._token_cache.get_async(force_refresh=True)
                conn = await open_connection(self._endpoint, self._token_cache, autocommit=False)
                log.debug("conn.recycle", age_s=round(time.monotonic() - opened_at, 1))
            except Exception as exc:
                # Open failed; retry loop will handle; return a closed sentinel.
                log.warning("conn.recycle.open_failed", error=str(exc))
                # Still return the (now closed) conn so the pool invariant holds;
                # the retry loop treats closed conns as retryable.
        self._conn_opened_mono[id(conn)] = time.monotonic()
        return conn

    async def _bootstrap_tables(self) -> None:
        if self._bootstrapped:
            return
        assert self._schema_conn is not None
        async with self._schema_conn.cursor() as cur:
            await cur.execute(_APPLIED_TABLE_DDL)
        await self._schema_conn.commit()
        await bootstrap_dlq_tables(self._schema_conn)
        await bootstrap_state_tables(self._schema_conn)
        self._bootstrapped = True

    # ---- apply ----------------------------------------------------------

    async def apply(
        self,
        txn: CompletedTransaction,
        *,
        pre_commit_hook: CursorHook | None = None,
    ) -> ApplyResult:
        """Apply one transaction atomically. Serializes per-key via stripe locks."""
        if self._schema is None:
            raise ApplyError("Applier.start() must be called before apply()")

        touched: set[int] | None = None
        for attempt in range(2):
            try:
                touched = await self._compute_touched_stripes(txn)
                break
            except SchemaNotFoundError as exc:
                raise ApplyError(f"schema missing on target: {exc}") from exc
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                if attempt == 0:
                    log.warning("schema_conn.reconnect", error=str(exc))
                    await self._refresh_schema_conn()
                    continue
                raise ApplyError(f"schema connection failure: {exc}") from exc
        assert touched is not None
        sorted_stripes = sorted(touched)

        async with AsyncExitStack() as stack:
            # Deadlock-free lock acquisition in sorted order.
            for s in sorted_stripes:
                await stack.enter_async_context(self._stripes[s])
            return await self._apply_with_retry(txn, pre_commit_hook, tuple(sorted_stripes))
        raise ApplyError(f"unreachable: AsyncExitStack escaped for tx_id={txn.tx_id}")

    async def apply_batch(
        self,
        txns: list[CompletedTransaction],
        *,
        pre_commit_hook: CursorHook | None = None,
    ) -> list[ApplyResult]:
        """Apply N source transactions in a single target-side BEGIN..COMMIT.

        See prior docstring for correctness contract. Timing instrumentation:
        every ~1-in-20 batch emits an `apply_batch.stage_timings` log line
        with ms spent in schema-compute vs. stripe-acquire vs. pool-wait vs.
        commit — this is the breakdown used to diagnose where the pipeline
        actually spends its wall-clock time. Added 2026-10-26 after client-
        side instrumentation showed 20% CPU + idle pool at 88 TPS cap.
        """
        if not txns:
            return []
        if self._schema is None:
            raise ApplyError("Applier.start() must be called before apply_batch()")

        t_schema0 = time.monotonic()
        touched: set[int] = set()
        for attempt in range(2):
            try:
                touched = set()
                for t in txns:
                    touched |= await self._compute_touched_stripes(t)
                break
            except SchemaNotFoundError as exc:
                raise ApplyError(f"schema missing on target: {exc}") from exc
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                if attempt == 0:
                    log.warning("schema_conn.reconnect", error=str(exc))
                    await self._refresh_schema_conn()
                    continue
                raise ApplyError(f"schema connection failure: {exc}") from exc
        schema_ms = (time.monotonic() - t_schema0) * 1000.0
        sorted_stripes = sorted(touched)

        t_stripe0 = time.monotonic()
        async with AsyncExitStack() as stack:
            for s in sorted_stripes:
                await stack.enter_async_context(self._stripes[s])
            stripe_ms = (time.monotonic() - t_stripe0) * 1000.0
            return await self._apply_batch_with_retry(
                txns,
                pre_commit_hook,
                tuple(sorted_stripes),
                _schema_ms=schema_ms,
                _stripe_ms=stripe_ms,
            )
        raise ApplyError(f"unreachable: AsyncExitStack escaped for batch of {len(txns)}")

    async def _apply_with_retry(
        self,
        txn: CompletedTransaction,
        pre_commit_hook: CursorHook | None,
        stripes: tuple[int, ...],
    ) -> ApplyResult:
        """Retry loop around `_apply_once`, scoped to a pool-borrowed connection."""
        last_exc: BaseException | None = None
        for attempt in range(1, self._max_retries + 1):
            conn = await self._borrow_fresh_conn()
            t0 = time.monotonic()
            try:
                rows_applied, rows_deduped = await self._apply_once(conn, txn, pre_commit_hook)
                return ApplyResult(
                    tx_id=txn.tx_id,
                    rows_applied=rows_applied,
                    rows_deduped=rows_deduped,
                    attempt=attempt,
                    duration_ms=(time.monotonic() - t0) * 1000.0,
                    stripes=stripes,
                )
            except _RETRYABLE as exc:
                last_exc = exc
                await self._rollback_quiet(conn)
                log.warning("apply.retryable", tx_id=txn.tx_id, attempt=attempt, error=str(exc))
                conn = await self._replace_conn(conn)
                if attempt < self._max_retries:
                    await asyncio.sleep(self._backoff(attempt))
            except SchemaNotFoundError as exc:
                await self._rollback_quiet(conn)
                raise ApplyError(f"schema missing on target: {exc}") from exc
            except Exception as exc:
                await self._rollback_quiet(conn)
                log.error("apply.permanent", tx_id=txn.tx_id, error=str(exc))
                raise ApplyError(f"non-retryable apply failure: {exc}") from exc
            finally:
                # Return the (possibly replaced) connection to the pool.
                await self._conn_pool.put(conn)
        raise ApplyError(f"exhausted {self._max_retries} retries for tx_id={txn.tx_id}: {last_exc}")

    async def _apply_once(
        self,
        conn: psycopg.AsyncConnection[tuple[object, ...]],
        txn: CompletedTransaction,
        pre_commit_hook: CursorHook | None = None,
    ) -> tuple[int, int]:
        """One attempt against a pool-borrowed connection."""
        assert self._schema is not None
        rows_applied = 0
        rows_deduped = 0
        async with conn.transaction(), conn.cursor() as cur:
            for rec in txn.records:
                stamped = await self._stamp(cur, txn.tx_id, _record_id(rec))
                if not stamped:
                    rows_deduped += 1
                    continue
                await self._apply_record(cur, rec)
                rows_applied += 1
                if self._fault_after_rows is not None and rows_applied >= self._fault_after_rows:
                    raise InjectedFaultError(f"injected fault after {rows_applied} row(s), before COMMIT")
            if pre_commit_hook is not None:
                await pre_commit_hook(cur)
        return rows_applied, rows_deduped

    async def _apply_batch_with_retry(
        self,
        txns: list[CompletedTransaction],
        pre_commit_hook: CursorHook | None,
        stripes: tuple[int, ...],
        *,
        _schema_ms: float = 0.0,
        _stripe_ms: float = 0.0,
    ) -> list[ApplyResult]:
        """Retry loop around `_apply_batch_once`, scoped to a pool-borrowed connection."""
        last_exc: BaseException | None = None
        for attempt in range(1, self._max_retries + 1):
            t_pool0 = time.monotonic()
            conn = await self._borrow_fresh_conn()
            pool_ms = (time.monotonic() - t_pool0) * 1000.0
            t0 = time.monotonic()
            try:
                per_txn = await self._apply_batch_once(conn, txns, pre_commit_hook)
                duration_ms = (time.monotonic() - t0) * 1000.0
                # Sampled per-stage breakdown so operators can see WHERE time
                # goes without flooding the log at every batch.
                if attempt == 1 and (hash(txns[0].tx_id) & 0x1F) == 0:
                    log.info(
                        "apply_batch.stage_timings",
                        size=len(txns),
                        schema_ms=round(_schema_ms, 2),
                        stripe_ms=round(_stripe_ms, 2),
                        pool_wait_ms=round(pool_ms, 2),
                        commit_ms=round(duration_ms, 2),
                        stripes_count=len(stripes),
                    )
                return [
                    ApplyResult(
                        tx_id=txn.tx_id,
                        rows_applied=applied,
                        rows_deduped=deduped,
                        attempt=attempt,
                        duration_ms=duration_ms,
                        stripes=stripes,
                    )
                    for txn, (applied, deduped) in zip(txns, per_txn, strict=True)
                ]
            except _RETRYABLE as exc:
                last_exc = exc
                await self._rollback_quiet(conn)
                log.warning(
                    "apply_batch.retryable",
                    size=len(txns),
                    attempt=attempt,
                    error=str(exc),
                )
                conn = await self._replace_conn(conn)
                if attempt < self._max_retries:
                    await asyncio.sleep(self._backoff(attempt))
            except SchemaNotFoundError as exc:
                await self._rollback_quiet(conn)
                raise ApplyError(f"schema missing on target: {exc}") from exc
            except Exception as exc:
                await self._rollback_quiet(conn)
                log.error("apply_batch.permanent", size=len(txns), error=str(exc))
                raise ApplyError(f"non-retryable batch failure: {exc}") from exc
            finally:
                await self._conn_pool.put(conn)
        raise ApplyError(f"exhausted {self._max_retries} retries for batch of {len(txns)}: {last_exc}")

    async def _apply_batch_once(
        self,
        conn: psycopg.AsyncConnection[tuple[object, ...]],
        txns: list[CompletedTransaction],
        pre_commit_hook: CursorHook | None,
    ) -> list[tuple[int, int]]:
        """One attempt against a pool-borrowed connection — one BEGIN..COMMIT for all txns."""
        assert self._schema is not None
        per_txn: list[tuple[int, int]] = []
        async with conn.transaction(), conn.cursor() as cur:
            for txn in txns:
                rows_applied = 0
                rows_deduped = 0
                for rec in txn.records:
                    stamped = await self._stamp(cur, txn.tx_id, _record_id(rec))
                    if not stamped:
                        rows_deduped += 1
                        continue
                    await self._apply_record(cur, rec)
                    rows_applied += 1
                    if self._fault_after_rows is not None and rows_applied >= self._fault_after_rows:
                        raise InjectedFaultError(f"injected fault after {rows_applied} row(s), before COMMIT")
                per_txn.append((rows_applied, rows_deduped))
            if pre_commit_hook is not None:
                await pre_commit_hook(cur)
        return per_txn

    # ---- key hashing & stripe computation -------------------------------

    async def _compute_touched_stripes(self, txn: CompletedTransaction) -> set[int]:
        """Collect the set of stripe ids touched by this txn. Loads schema if needed."""
        assert self._schema is not None
        stripes: set[int] = set()
        for rec in txn.records:
            source = _source_of(rec)
            meta = await self._schema.get(source.schema_name, source.table)
            payload = _after_of(rec) or _before_of(rec)
            if not payload:
                continue
            pk_values = tuple(str(payload.get(pk, "")) for pk in meta.pk_columns)
            key = (source.schema_name, source.table, pk_values)
            stripes.add(_stripe_of(key, self._stripe_count))
        return stripes

    async def _stamp(self, cur: psycopg.AsyncCursor[Any], tx_id: str, record_id: str) -> bool:
        await cur.execute(
            "INSERT INTO __replicator_applied (tx_id, record_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (tx_id, record_id),
        )
        return cur.rowcount == 1

    async def _apply_record(self, cur: psycopg.AsyncCursor[Any], rec: ReadyRecord) -> None:
        assert self._schema is not None
        source = _source_of(rec)
        meta = await self._schema.get(source.schema_name, source.table)
        op = _op_of(rec)
        if op == "d":
            await self._delete(cur, meta, _before_of(rec))
        else:
            await self._upsert(cur, meta, _after_of(rec))

    async def _delete(
        self, cur: psycopg.AsyncCursor[Any], meta: TableMeta, before: dict[str, Any] | None
    ) -> None:
        if not before:
            raise ApplyError(f"delete record for {meta.qualified} has no `before` payload")
        for pk in meta.pk_columns:
            if pk not in before:
                raise ApplyError(f"delete for {meta.qualified} missing PK column {pk}")
        where = sql.SQL(" AND ").join(sql.SQL("{} = %s").format(sql.Identifier(pk)) for pk in meta.pk_columns)
        statement = sql.SQL("DELETE FROM {} WHERE {}").format(sql.SQL(meta.qualified), where)
        params = tuple(coerce_value(meta.type_of(pk) or "", before[pk]) for pk in meta.pk_columns)
        await cur.execute(statement, params)

    async def _upsert(
        self, cur: psycopg.AsyncCursor[Any], meta: TableMeta, after: dict[str, Any] | None
    ) -> None:
        if not after:
            raise ApplyError(f"insert/update record for {meta.qualified} has no `after` payload")
        cols: list[str] = [c.name for c in meta.columns if c.name in after]
        if not cols:
            raise ApplyError(f"upsert for {meta.qualified} has no overlapping columns")
        for pk in meta.pk_columns:
            if pk not in after:
                raise ApplyError(f"upsert for {meta.qualified} missing PK column {pk}")

        placeholders = sql.SQL(", ").join(sql.SQL("%s") for _ in cols)
        col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
        pk_idents = sql.SQL(", ").join(sql.Identifier(c) for c in meta.pk_columns)
        update_set = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in cols
            if c not in meta.pk_columns
        )
        if update_set.as_string(cur).strip():
            on_conflict = sql.SQL("ON CONFLICT ({}) DO UPDATE SET {}").format(pk_idents, update_set)
        else:
            on_conflict = sql.SQL("ON CONFLICT ({}) DO NOTHING").format(pk_idents)

        statement = sql.SQL("INSERT INTO {} ({}) VALUES ({}) {}").format(
            sql.SQL(meta.qualified), col_idents, placeholders, on_conflict
        )
        params = tuple(coerce_value(meta.type_of(c) or "", after[c]) for c in cols)
        await cur.execute(statement, params)

    # ---- helpers --------------------------------------------------------

    async def _rollback_quiet(self, conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
        if conn.closed:
            return
        try:
            if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                await conn.rollback()
        except Exception as exc:
            log.debug("apply.rollback.quiet", error=str(exc))

    async def _replace_conn(
        self, old: psycopg.AsyncConnection[tuple[object, ...]]
    ) -> psycopg.AsyncConnection[tuple[object, ...]]:
        """Close a broken connection + open a fresh one with refreshed token.

        Does its own bounded retry loop (3 attempts, exp backoff) before giving up.
        On total failure returns the (now-closed) old conn — the outer retry loop
        will treat it as `_RETRYABLE` and come back here on the next attempt.
        The old `return old` path with a LIVE broken socket caused the 2026-10-26
        SSL-EOF fence storm: all 5 outer retries looped on the same dead socket.
        """
        self._conn_opened_mono.pop(id(old), None)
        if not old.closed:
            try:
                await old.close()
            except Exception as exc:
                log.debug("apply.close.quiet", error=str(exc))
        for inner in range(1, 4):  # 3 attempts, bounded so caller's budget still shrinks
            try:
                await self._token_cache.get_async(force_refresh=True)
                new_conn = await open_connection(self._endpoint, self._token_cache, autocommit=False)
                self._conn_opened_mono[id(new_conn)] = time.monotonic()
                return new_conn
            except Exception as exc:
                log.warning(
                    "apply.reconnect.failed",
                    attempt=inner,
                    error=str(exc),
                )
                if inner < 3:
                    await asyncio.sleep(self._backoff(inner))
        # All inner attempts failed. Return the closed `old` — the outer except
        # block's next iteration will catch IOError on it and come back here.
        return old

    def _backoff(self, attempt: int) -> float:
        upper = min(self._backoff_cap, self._backoff_initial * (2 ** (attempt - 1)))
        return random.uniform(0.0, upper)

    async def _refresh_schema_conn(self) -> None:
        """Close + reopen the schema connection; swap it into SchemaCache. Called when
        the schema conn's DSQL token has expired and the server has closed the socket."""
        assert self._schema is not None
        old = self._schema_conn
        self._schema_conn = None
        if old is not None and not old.closed:
            try:
                await old.close()
            except Exception as exc:
                log.debug("schema_conn.close.quiet", error=str(exc))
        await self._token_cache.get_async(force_refresh=True)
        self._schema_conn = await open_connection(self._endpoint, self._token_cache, autocommit=False)
        self._schema.set_conn(self._schema_conn)
        log.info("schema_conn.refreshed")


# ---------- small helpers over ReadyRecord ----------


def _record_id(rec: ReadyRecord) -> str:
    if isinstance(rec, AssembledRecord):
        return rec.main.transaction.record_id
    return rec.transaction.record_id


def _source_of(rec: ReadyRecord) -> Any:
    if isinstance(rec, AssembledRecord):
        return rec.main.source
    return rec.source


def _op_of(rec: ReadyRecord) -> str:
    if isinstance(rec, AssembledRecord):
        return rec.main.op
    return rec.op


def _after_of(rec: ReadyRecord) -> dict[str, Any] | None:
    if isinstance(rec, AssembledRecord):
        return rec.after if isinstance(rec.after, dict) else None
    v = rec.after
    return v if isinstance(v, dict) else None


def _before_of(rec: ReadyRecord) -> dict[str, Any] | None:
    if isinstance(rec, AssembledRecord):
        return rec.before if isinstance(rec.before, dict) else None
    v = rec.before
    return v if isinstance(v, dict) else None


def _stripe_of(key: tuple[str, str, tuple[str, ...]], stripe_count: int) -> int:
    """Stable hash of a (schema, table, pk-tuple) to a stripe id in [0, stripe_count).

    Uses blake2b with a 2-byte digest — plenty for 64 stripes with low collision rate.
    """
    h = hashlib.blake2b(digest_size=2)
    h.update(key[0].encode("utf-8"))
    h.update(b"\0")
    h.update(key[1].encode("utf-8"))
    h.update(b"\0")
    for v in key[2]:
        h.update(v.encode("utf-8"))
        h.update(b"\0")
    return int.from_bytes(h.digest(), "big") % stripe_count


# Silence unused imports kept for forward reference.
_ = (FullRecord, ChunkedRecord)
