"""Target-DSQL schema for the DLQ fence subsystem.

Two tables, both in the target cluster alongside `__replicator_applied`:

- `__replicator_dlq_fences`: one row per fence event. `status` cycles OPEN → CLOSED
  (on retry success) or OPEN → SKIPPED (on `ops dlq skip --confirm`).

- `__replicator_dlq_payload`: full `CompletedTransaction` payload stored as JSONB so an
  operator can `ops dlq inspect/retry/edit-and-retry` without needing Kinesis replay.
  Keyed by `tx_id`; cascade is intentional — closing/skipping a fence keeps the payload
  for forensic lookup (the row can be deleted explicitly later if desired).

Bootstrap is idempotent (CREATE TABLE IF NOT EXISTS); safe to call on every process start.
"""

from __future__ import annotations

import psycopg
import structlog

log = structlog.get_logger("dlq.schema")

FENCES_DDL = """
CREATE TABLE IF NOT EXISTS __replicator_dlq_fences (
    tx_id         text        PRIMARY KEY,
    fence_ts_ns   bigint      NOT NULL,
    opened_at     timestamptz NOT NULL DEFAULT now(),
    closed_at     timestamptz,
    status        text        NOT NULL,
    error_class   text        NOT NULL,
    failing_sql   text        NOT NULL
)
"""

PAYLOAD_DDL = """
CREATE TABLE IF NOT EXISTS __replicator_dlq_payload (
    tx_id        text  PRIMARY KEY,
    payload_json jsonb NOT NULL
)
"""


async def bootstrap_dlq_tables(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
    """Create the DLQ fence + payload tables if they don't exist. Idempotent.

    DSQL only accepts one DDL statement per transaction (`FeatureNotSupported: multiple
    ddl statements not supported in a transaction`), so each CREATE runs in its own
    implicit transaction.
    """
    async with conn.cursor() as cur:
        await cur.execute(FENCES_DDL)
        await conn.commit()
        await cur.execute(PAYLOAD_DDL)
        await conn.commit()
    log.info("dlq.schema.ready")
