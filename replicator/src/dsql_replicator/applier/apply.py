"""Applier: per-row last-writer-wins upsert into the target DSQL cluster.

Row-level eventual consistency (post-2026-11 simplification pass). For each
record we write the row and bump `__replicator_lww_stamps` for its primary key,
gated by:

    INSERT INTO __replicator_lww_stamps VALUES (schema, table, pk, ts_ns)
    ON CONFLICT (schema, table, pk) DO UPDATE
       SET applied_ts_ns = EXCLUDED.applied_ts_ns, applied_at = now()
     WHERE __replicator_lww_stamps.applied_ts_ns < EXCLUDED.applied_ts_ns

If the stamp advances we run the row DML in the same transaction; otherwise
the record is silently discarded. Rows converge to the latest source commit
per primary key. **Transactions do NOT converge** — two source commits
touching different rows can land on the replica in any order, so the replica
may briefly show a state that never existed at the source. See
docs/DESIGN.md for the full contract.

Retries are **infinite** with exponential backoff capped at `backoff_cap_s`.
A persistent apply failure blocks only its own stripe locks, letting other
keys keep flowing.

- Stripe locks (default 64) serialize per-key apply so concurrent workers
  don't thrash with OCC conflicts on the same primary key.
- Connection pool of `worker_count` dedicated psycopg3 connections, grown /
  shrunk at runtime by the writer autoscaler.
- The stamp table is kept **separate from customer tables** — stamps are
  never inlined into customer rows, so the solution is non-invasive.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
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

from dsql_replicator.envelope import ChunkedRecord, FullRecord
from dsql_replicator.errors import ReplicatorError
from dsql_replicator.reassembler import AssembledRecord, ReadyRecord
from dsql_replicator.state.schema import bootstrap_state_tables

from .coerce import coerce_value
from .connection import DsqlEndpoint, DsqlTokenCache, open_connection
from .schema import SchemaCache, SchemaNotFoundError, TableMeta

CursorHook = Callable[[psycopg.AsyncCursor[Any]], Awaitable[None]]

log = structlog.get_logger("applier")

_STAMPS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS __replicator_lww_stamps (
    schema_name   text NOT NULL,
    table_name    text NOT NULL,
    pk            text NOT NULL,
    applied_ts_ns bigint NOT NULL,
    applied_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, table_name, pk)
)
"""


class ApplyError(ReplicatorError):
    """Non-retryable apply failure (schema missing on target, malformed record).

    Transient infrastructure failures do NOT raise this — they retry forever
    with exponential backoff inside `apply_batch`."""


@dataclass(frozen=True)
class ApplyResult:
    rows_applied: int
    rows_discarded: int  # LWW-rejected or crash-replay dedup
    attempt: int
    duration_ms: float
    stripes: tuple[int, ...] = ()


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
        backoff_initial_s: float = 0.1,
        backoff_cap_s: float = 10.0,
        stripe_count: int = 64,
        worker_count: int = 32,
        max_worker_count: int | None = None,
    ) -> None:
        self._endpoint = endpoint
        self._token_cache = DsqlTokenCache(endpoint)
        self._backoff_initial = backoff_initial_s
        self._backoff_cap = backoff_cap_s
        self._stripe_count = max(1, stripe_count)
        self._worker_count = max(1, worker_count)
        self._max_worker_count = max(self._worker_count, max_worker_count or self._worker_count)
        self._stripes: list[asyncio.Lock] = [asyncio.Lock() for _ in range(self._stripe_count)]
        self._conn_pool: asyncio.Queue[psycopg.AsyncConnection[tuple[object, ...]]] = asyncio.Queue(
            maxsize=self._max_worker_count
        )
        self._pool_resize_lock = asyncio.Lock()
        # DSQL IAM tokens expire after ~15 min. Pre-emptively recycle any pool
        # connection that is older than this before handing it out, else a
        # quiet pool returns dead sockets after an idle window.
        self._conn_opened_mono: dict[int, float] = {}
        self._conn_max_age_s: float = 12 * 60.0
        self._schema_conn: psycopg.AsyncConnection[tuple[object, ...]] | None = None
        self._schema: SchemaCache | None = None
        self._bootstrapped = False

    # ---- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        if self._schema_conn is not None:
            return
        self._schema_conn = await open_connection(self._endpoint, self._token_cache, autocommit=False)
        self._schema = SchemaCache(self._schema_conn)
        await self._bootstrap_tables()
        # Staggered pre-warm so a large pool doesn't hit the DSQL endpoint
        # with correlated token-mint + TLS handshake bursts.
        for i in range(self._worker_count):
            conn = await open_connection(self._endpoint, self._token_cache, autocommit=False)
            self._conn_opened_mono[id(conn)] = time.monotonic()
            await self._conn_pool.put(conn)
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

    async def _bootstrap_tables(self) -> None:
        if self._bootstrapped:
            return
        assert self._schema_conn is not None
        # DSQL rejects multiple DDL per transaction; commit after each CREATE.
        async with self._schema_conn.cursor() as cur:
            await cur.execute(_STAMPS_TABLE_DDL)
        await self._schema_conn.commit()
        await bootstrap_state_tables(self._schema_conn)
        self._bootstrapped = True

    # ---- dynamic pool resizing (used by WorkerAutoscaler) ---------------

    @property
    def worker_count(self) -> int:
        return self._worker_count

    @property
    def max_worker_count(self) -> int:
        return self._max_worker_count

    async def grow_pool(self, delta: int) -> int:
        if delta <= 0:
            return 0
        async with self._pool_resize_lock:
            room = self._max_worker_count - self._worker_count
            to_add = min(delta, room)
            if to_add <= 0:
                return 0
            for _ in range(to_add):
                conn = await open_connection(self._endpoint, self._token_cache, autocommit=False)
                self._conn_opened_mono[id(conn)] = time.monotonic()
                await self._conn_pool.put(conn)
            self._worker_count += to_add
            return to_add

    async def shrink_pool(self, delta: int) -> int:
        if delta <= 0 or self._worker_count <= 1:
            return 0
        async with self._pool_resize_lock:
            floor_room = self._worker_count - 1
            to_remove = min(delta, floor_room, self._conn_pool.qsize())
            removed = 0
            for _ in range(to_remove):
                try:
                    conn = self._conn_pool.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._conn_opened_mono.pop(id(conn), None)
                if not conn.closed:
                    await conn.close()
                removed += 1
            self._worker_count -= removed
            return removed

    async def _borrow_fresh_conn(self) -> psycopg.AsyncConnection[tuple[object, ...]]:
        conn = await self._conn_pool.get()
        opened_at = self._conn_opened_mono.pop(id(conn), 0.0)
        if conn.closed or (time.monotonic() - opened_at) >= self._conn_max_age_s:
            if not conn.closed:
                with contextlib.suppress(Exception):
                    await conn.close()
            try:
                await self._token_cache.get_async(force_refresh=True)
                conn = await open_connection(self._endpoint, self._token_cache, autocommit=False)
                log.debug("conn.recycle", age_s=round(time.monotonic() - opened_at, 1))
            except Exception as exc:
                log.warning("conn.recycle.open_failed", error=str(exc))
        self._conn_opened_mono[id(conn)] = time.monotonic()
        return conn

    # ---- apply ----------------------------------------------------------

    async def apply_batch(
        self,
        records: list[ReadyRecord],
        *,
        pre_commit_hook: CursorHook | None = None,
    ) -> ApplyResult:
        """Apply a batch of records in one target BEGIN..COMMIT with per-row LWW.

        The batch acquires stripe locks for the union of touched primary keys
        (in sorted order to avoid deadlock), then runs:

            for rec in records:
                stamped = INSERT ... ON CONFLICT DO UPDATE ... WHERE older_ts_ns
                if stamped: apply row DML
            pre_commit_hook(cursor)
            COMMIT

        Retries forever on transient DSQL failures with exponential backoff.
        Only raises `ApplyError` for structural problems (schema missing on
        target) that are not fixable by retrying.
        """
        if not records:
            return ApplyResult(0, 0, 1, 0.0, ())
        if self._schema is None:
            raise ApplyError("Applier.start() must be called before apply_batch()")

        touched: set[int] = set()
        for attempt in range(2):
            try:
                touched = set()
                for rec in records:
                    touched |= await self._touched_stripes(rec)
                break
            except SchemaNotFoundError as exc:
                raise ApplyError(f"schema missing on target: {exc}") from exc
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                if attempt == 0:
                    log.warning("schema_conn.reconnect", error=str(exc))
                    await self._refresh_schema_conn()
                    continue
                raise ApplyError(f"schema connection failure: {exc}") from exc
        sorted_stripes = tuple(sorted(touched))

        async with AsyncExitStack() as stack:
            for s in sorted_stripes:
                await stack.enter_async_context(self._stripes[s])
            return await self._apply_with_retry(records, pre_commit_hook, sorted_stripes)
        raise ApplyError("unreachable: AsyncExitStack escaped apply_batch")

    async def _apply_with_retry(
        self,
        records: list[ReadyRecord],
        pre_commit_hook: CursorHook | None,
        stripes: tuple[int, ...],
    ) -> ApplyResult:
        """Infinite retry loop around `_apply_once`.

        Transient DSQL failures retry forever with exponential backoff. Only
        `ApplyError` from `_apply_once` (schema missing / malformed record)
        escapes to the caller — those are not fixable by retrying."""
        attempt = 0
        while True:
            attempt += 1
            conn = await self._borrow_fresh_conn()
            t0 = time.monotonic()
            try:
                rows_applied, rows_discarded = await self._apply_once(conn, records, pre_commit_hook)
                return ApplyResult(
                    rows_applied=rows_applied,
                    rows_discarded=rows_discarded,
                    attempt=attempt,
                    duration_ms=(time.monotonic() - t0) * 1000.0,
                    stripes=stripes,
                )
            except _RETRYABLE as exc:
                await self._rollback_quiet(conn)
                log.warning("apply.retryable", size=len(records), attempt=attempt, error=str(exc))
                conn = await self._replace_conn(conn)
                await asyncio.sleep(self._backoff(attempt))
            except ApplyError:
                await self._rollback_quiet(conn)
                raise
            except Exception as exc:
                # Unexpected but we still don't fence — log loudly and retry.
                await self._rollback_quiet(conn)
                log.error(
                    "apply.unexpected_retrying",
                    size=len(records),
                    attempt=attempt,
                    error=str(exc),
                    error_class=type(exc).__name__,
                )
                await asyncio.sleep(self._backoff(attempt))
            finally:
                await self._conn_pool.put(conn)

    async def _apply_once(
        self,
        conn: psycopg.AsyncConnection[tuple[object, ...]],
        records: list[ReadyRecord],
        pre_commit_hook: CursorHook | None,
    ) -> tuple[int, int]:
        """One attempt against a pool-borrowed connection.

        Returns `(rows_applied, rows_discarded)`."""
        assert self._schema is not None
        rows_applied = 0
        rows_discarded = 0
        async with conn.transaction(), conn.cursor() as cur:
            for rec in records:
                schema_name, table_name, pk_text, ts_ns = await self._record_key_tuple(rec)
                stamped = await self._lww_stamp(cur, schema_name, table_name, pk_text, ts_ns)
                if not stamped:
                    rows_discarded += 1
                    continue
                await self._apply_record(cur, rec)
                rows_applied += 1
            if pre_commit_hook is not None:
                await pre_commit_hook(cur)
        return rows_applied, rows_discarded

    # ---- LWW stamp ------------------------------------------------------

    async def _lww_stamp(
        self,
        cur: psycopg.AsyncCursor[Any],
        schema_name: str,
        table_name: str,
        pk_text: str,
        ts_ns: int,
    ) -> bool:
        """Advance the LWW stamp for a primary key. Returns True iff advanced.

        A False return means an equal-or-newer ts_ns was already recorded —
        the caller must skip the row DML (either a late-arriving older commit
        or a crash-replay duplicate)."""
        await cur.execute(
            """
            INSERT INTO __replicator_lww_stamps
                (schema_name, table_name, pk, applied_ts_ns)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (schema_name, table_name, pk) DO UPDATE
                SET applied_ts_ns = EXCLUDED.applied_ts_ns,
                    applied_at    = now()
                WHERE __replicator_lww_stamps.applied_ts_ns < EXCLUDED.applied_ts_ns
            """,
            (schema_name, table_name, pk_text, ts_ns),
        )
        return cur.rowcount == 1

    async def _record_key_tuple(self, rec: ReadyRecord) -> tuple[str, str, str, int]:
        assert self._schema is not None
        source = _source_of(rec)
        meta = await self._schema.get(source.schema_name, source.table)
        payload = _after_of(rec) or _before_of(rec) or {}
        pk_values = [str(payload.get(pk, "")) for pk in meta.pk_columns]
        pk_text = json.dumps(pk_values, separators=(",", ":"))
        return source.schema_name, source.table, pk_text, int(source.ts_ns)

    async def _touched_stripes(self, rec: ReadyRecord) -> set[int]:
        assert self._schema is not None
        source = _source_of(rec)
        meta = await self._schema.get(source.schema_name, source.table)
        payload = _after_of(rec) or _before_of(rec) or {}
        pk_values = tuple(str(payload.get(pk, "")) for pk in meta.pk_columns)
        key = (source.schema_name, source.table, pk_values)
        return {_stripe_of(key, self._stripe_count)}

    # ---- row DML --------------------------------------------------------

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
        self._conn_opened_mono.pop(id(old), None)
        if not old.closed:
            try:
                await old.close()
            except Exception as exc:
                log.debug("apply.close.quiet", error=str(exc))
        for inner in range(1, 4):
            try:
                await self._token_cache.get_async(force_refresh=True)
                new_conn = await open_connection(self._endpoint, self._token_cache, autocommit=False)
                self._conn_opened_mono[id(new_conn)] = time.monotonic()
                return new_conn
            except Exception as exc:
                log.warning("apply.reconnect.failed", attempt=inner, error=str(exc))
                if inner < 3:
                    await asyncio.sleep(self._backoff(inner))
        return old

    def _backoff(self, attempt: int) -> float:
        upper = min(self._backoff_cap, self._backoff_initial * (2 ** (attempt - 1)))
        return random.uniform(0.0, upper)

    async def _refresh_schema_conn(self) -> None:
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
    h = hashlib.blake2b(digest_size=2)
    h.update(key[0].encode("utf-8"))
    h.update(b"\0")
    h.update(key[1].encode("utf-8"))
    h.update(b"\0")
    for v in key[2]:
        h.update(v.encode("utf-8"))
        h.update(b"\0")
    return int.from_bytes(h.digest(), "big") % stripe_count


# Keep forward-reference imports live so callers that re-export them do not
# break on the applier module boundary.
_ = (FullRecord, ChunkedRecord)
