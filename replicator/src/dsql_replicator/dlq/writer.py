"""DLQ fence writer: opens / closes / skips fence rows in target DSQL.

State plane lives in two target-DSQL tables (see `dlq/schema.py`):

- `__replicator_dlq_fences(tx_id PK, fence_ts_ns, opened_at, closed_at, status, error_class, failing_sql)`
- `__replicator_dlq_payload(tx_id PK, payload_json jsonb)`

`open_fence` serializes the full `CompletedTransaction` into the payload table and marks
the fence OPEN; `close_fence_in_cursor` runs INSIDE a caller's cursor so retry can close
the fence inside the same DSQL transaction as the row apply (atomic exactly-once retry,
parallel to the 2026-10-09 idempotency decision). `skip_fence` is non-atomic — operator
decision only.

The writer owns its own target-DSQL connection and its own SNS client; it does NOT share
with the Applier (which holds the commit lane). The retry path explicitly bridges the
two by passing the Applier's cursor into `close_fence_in_cursor`.

SNS publishes are fire-and-forget — the target-DSQL fence table is the authoritative
source of truth; SNS exists only to notify operators.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

import boto3
import psycopg
import structlog
from psycopg.types.json import Jsonb

from dsql_replicator.applier.connection import DsqlEndpoint, DsqlTokenCache, open_connection
from dsql_replicator.config import Config
from dsql_replicator.envelope import ChunkedRecord, FullRecord
from dsql_replicator.reassembler import AssembledRecord, CompletedTransaction, ReadyRecord

log = structlog.get_logger("dlq.writer")


class FenceStatus(StrEnum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class OpenFenceRow:
    tx_id: str
    fence_ts_ns: int
    opened_at: datetime
    status: FenceStatus
    error_class: str
    failing_sql: str


class DlqWriter:
    """Writes DLQ fence state to target DSQL and fires SNS notifications."""

    def __init__(self, cfg: Config, endpoint: DsqlEndpoint) -> None:
        self._cfg = cfg
        self._endpoint = endpoint
        self._token_cache = DsqlTokenCache(endpoint)
        self._conn: psycopg.AsyncConnection[tuple[object, ...]] | None = None
        self._sns = boto3.client("sns", region_name=cfg.control_region)
        self._topic_arn: str | None = None

    # ---- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        """Open own connection + resolve SNS topic ARN (idempotent).

        Autocommit is TRUE so individual SELECTs don't park an implicit transaction
        that would force `conn.transaction()` to emit SAVEPOINT — DSQL rejects
        SAVEPOINT (`FeatureNotSupported: unsupported transaction statement`).
        """
        if self._conn is None:
            self._conn = await open_connection(self._endpoint, self._token_cache, autocommit=True)
        if self._topic_arn is None:
            # CDK owns topic creation + KMS (M6.4). We still call `create_topic` here
            # because AWS makes it idempotent — returns the existing ARN without
            # modifying attributes if the topic already exists. This keeps the task
            # role identical across M3-M6.0 and avoids a rollout-ordering problem where
            # a new role rolls out before the new image under circuit-breaker control.
            resp = await asyncio.to_thread(self._sns.create_topic, Name=self._cfg.dlq_sns_topic_name)
            self._topic_arn = str(resp["TopicArn"])
            log.info("dlq.writer.ready", topic_arn=self._topic_arn)

    async def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            await self._conn.close()
        self._conn = None

    def _require_conn(self) -> psycopg.AsyncConnection[tuple[object, ...]]:
        """Sync accessor for short-lived callers (ops CLI). For long-running loops use
        the methods below which auto-reconnect on connection errors."""
        if self._conn is None:
            raise RuntimeError("DlqWriter.start() must be called before use")
        return self._conn

    async def _ensure_conn(self) -> psycopg.AsyncConnection[tuple[object, ...]]:
        if self._conn is None or self._conn.closed:
            await self._refresh_conn()
        assert self._conn is not None
        return self._conn

    async def _refresh_conn(self) -> None:
        """Close any existing connection and open a fresh one with a refreshed token.

        Fixes the long-running "connection is closed" wedge when DSQL token expires
        at ~15 min (M6.4 post-500TPS finding)."""
        old = self._conn
        self._conn = None
        if old is not None and not old.closed:
            with contextlib.suppress(Exception):
                await old.close()
        self._token_cache.get(force_refresh=True)
        self._conn = await open_connection(self._endpoint, self._token_cache, autocommit=True)
        log.info("dlq.conn.refreshed")

    async def _run(self, func: Any) -> Any:
        """Run ``await func(conn)`` with one reconnect retry on connection errors."""
        for attempt in range(2):
            try:
                conn = await self._ensure_conn()
                return await func(conn)
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                if attempt == 0:
                    log.warning("dlq.conn.reconnect", error=str(exc))
                    await self._refresh_conn()
                    continue
                raise
        raise RuntimeError("unreachable: dlq._run exhausted retries without raising")

    # ---- fence open -----------------------------------------------------

    async def open_fence(
        self,
        txn: CompletedTransaction,
        *,
        error_class: str,
        failing_sql: str,
    ) -> None:
        """Open a fence atomically: fence row + payload row in one DSQL transaction."""
        payload = _serialize_txn(txn)

        async def body(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
            async with conn.transaction(), conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO __replicator_dlq_fences "
                    "(tx_id, fence_ts_ns, status, error_class, failing_sql) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (tx_id) DO UPDATE SET "
                    "  fence_ts_ns = EXCLUDED.fence_ts_ns, "
                    "  status      = EXCLUDED.status, "
                    "  error_class = EXCLUDED.error_class, "
                    "  failing_sql = EXCLUDED.failing_sql, "
                    "  opened_at   = now(), "
                    "  closed_at   = NULL",
                    (txn.tx_id, txn.ts_ns, FenceStatus.OPEN.value, error_class, failing_sql),
                )
                await cur.execute(
                    "INSERT INTO __replicator_dlq_payload (tx_id, payload_json) "
                    "VALUES (%s, %s) "
                    "ON CONFLICT (tx_id) DO UPDATE SET payload_json = EXCLUDED.payload_json",
                    (txn.tx_id, Jsonb(payload)),
                )

        await self._run(body)
        log.warning(
            "dlq.fence.opened",
            tx_id=txn.tx_id,
            fence_ts_ns=txn.ts_ns,
            error_class=error_class,
        )
        await self._publish_sns(
            event_type="opened",
            tx_id=txn.tx_id,
            fence_ts_ns=txn.ts_ns,
            error_class=error_class,
        )

    # ---- fence close ----------------------------------------------------

    async def close_fence_in_cursor(self, cur: psycopg.AsyncCursor[Any], tx_id: str) -> int:
        """Flip fence row to CLOSED using the caller's cursor (same txn as retry apply).

        Returns the number of rows updated (0 if no OPEN fence found)."""
        await cur.execute(
            "UPDATE __replicator_dlq_fences "
            "SET status = %s, closed_at = now() "
            "WHERE tx_id = %s AND status = %s",
            (FenceStatus.CLOSED.value, tx_id, FenceStatus.OPEN.value),
        )
        return int(cur.rowcount)

    async def skip_fence(self, tx_id: str, *, approver: str) -> bool:
        """Flip OPEN fence to SKIPPED. Returns True if a row was updated."""

        async def body(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> int:
            async with conn.transaction(), conn.cursor() as cur:
                await cur.execute(
                    "UPDATE __replicator_dlq_fences "
                    "SET status = %s, closed_at = now(), "
                    "    failing_sql = failing_sql || %s "
                    "WHERE tx_id = %s AND status = %s",
                    (
                        FenceStatus.SKIPPED.value,
                        f"\n-- SKIPPED by {approver} at {datetime.utcnow().isoformat()}Z",
                        tx_id,
                        FenceStatus.OPEN.value,
                    ),
                )
                return int(cur.rowcount)

        updated = int(await self._run(body))
        if updated:
            log.warning("dlq.fence.skipped", tx_id=tx_id, approver=approver)
            await self._publish_sns(event_type="skipped", tx_id=tx_id, approver=approver)
        return updated > 0

    async def publish_closed(self, tx_id: str) -> None:
        """Side-effect SNS publish after an in-transaction close succeeds."""
        await self._publish_sns(event_type="closed", tx_id=tx_id)

    # ---- queries --------------------------------------------------------

    async def fetch_open_fence(self) -> OpenFenceRow | None:
        """Return the oldest OPEN fence, or None. Used by consumer poller."""

        async def body(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> Any:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT tx_id, fence_ts_ns, opened_at, status, error_class, failing_sql "
                    "FROM __replicator_dlq_fences "
                    "WHERE status = %s "
                    "ORDER BY fence_ts_ns ASC "
                    "LIMIT 1",
                    (FenceStatus.OPEN.value,),
                )
                return await cur.fetchone()

        row = await self._run(body)
        if row is None:
            return None
        return _row_to_fence(row)

    async def list_fences(self, *, status: FenceStatus | None = None) -> list[OpenFenceRow]:
        where = ""
        params: tuple[Any, ...] = ()
        if status is not None:
            where = "WHERE status = %s "
            params = (status.value,)

        async def body(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> list[Any]:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT tx_id, fence_ts_ns, opened_at, status, error_class, failing_sql "
                    f"FROM __replicator_dlq_fences {where}"
                    "ORDER BY fence_ts_ns ASC",
                    params,
                )
                return list(await cur.fetchall())

        rows = await self._run(body)
        return [_row_to_fence(r) for r in rows]

    async def fetch_payload(self, tx_id: str) -> dict[str, Any] | None:
        async def body(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> Any:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT payload_json FROM __replicator_dlq_payload WHERE tx_id = %s",
                    (tx_id,),
                )
                return await cur.fetchone()

        row = await self._run(body)
        if row is None:
            return None
        v = row[0]
        return v if isinstance(v, dict) else None

    # ---- SNS ------------------------------------------------------------

    async def _publish_sns(self, *, event_type: str, **fields: Any) -> None:
        """Fire-and-forget SNS publish. Logs on failure; never raises."""
        if self._topic_arn is None:
            log.warning("dlq.sns.skipped", reason="topic_not_resolved", event_type=event_type)
            return
        body = {
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            **fields,
        }
        try:
            await asyncio.to_thread(
                self._sns.publish,
                TopicArn=self._topic_arn,
                Message=json.dumps(body),
                Subject=f"DSQL replicator DLQ: {event_type}",
                MessageAttributes={
                    "event_type": {"DataType": "String", "StringValue": event_type},
                },
            )
        except Exception as exc:
            log.warning("dlq.sns.publish_failed", event_type=event_type, error=str(exc))


# ---------- serialization helpers ----------


def _row_to_fence(row: Any) -> OpenFenceRow:
    return OpenFenceRow(
        tx_id=str(row[0]),
        fence_ts_ns=int(row[1]),
        opened_at=row[2],
        status=FenceStatus(row[3]),
        error_class=str(row[4]),
        failing_sql=str(row[5]),
    )


def _serialize_txn(txn: CompletedTransaction) -> dict[str, Any]:
    return {
        "tx_id": txn.tx_id,
        "ts_ns": txn.ts_ns,
        "total_records": txn.total_records,
        "records": [_serialize_record(r) for r in txn.records],
    }


def _serialize_record(rec: Any) -> dict[str, Any]:
    if isinstance(rec, AssembledRecord):
        return {
            "kind": "assembled",
            "main": rec.main.model_dump(mode="json", by_alias=True),
            "before": rec.before,
            "after": rec.after,
        }
    return {"kind": "full", "payload": rec.model_dump(mode="json", by_alias=True)}


def deserialize_txn(payload: dict[str, Any]) -> CompletedTransaction:
    """Rebuild a `CompletedTransaction` from a `__replicator_dlq_payload.payload_json` row."""
    records: tuple[ReadyRecord, ...] = tuple(_deserialize_record(r) for r in payload["records"])
    return CompletedTransaction(
        tx_id=str(payload["tx_id"]),
        ts_ns=int(payload["ts_ns"]),
        total_records=int(payload["total_records"]),
        records=records,
    )


def _deserialize_record(entry: dict[str, Any]) -> ReadyRecord:
    kind = entry.get("kind")
    if kind == "full":
        return FullRecord.model_validate(entry["payload"])
    if kind == "assembled":
        main = ChunkedRecord.model_validate(entry["main"])
        return AssembledRecord(main=main, before=entry.get("before"), after=entry.get("after"))
    raise ValueError(f"unknown record kind in payload: {kind!r}")
