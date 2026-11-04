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

# Ingest-side rejects (parse errors, CRC mismatches, fragment/txn timeouts). These
# never reached the applier, so there is no `tx_id`-keyed CompletedTransaction to
# fence on — they are archived for forensics only, not a commit barrier. Keyed by
# a UUID because the natural identifiers (chunk_id, tx_id) are often absent at
# reject time (parse_error has no tx_id). See review doc issue #2.
INGEST_REJECTS_DDL = """
CREATE TABLE IF NOT EXISTS __replicator_dlq_ingest (
    reject_id     uuid        PRIMARY KEY,
    stream_name   text        NOT NULL,
    shard_id      text,
    reason        text        NOT NULL,
    tx_id         text,
    record_id     text,
    chunk_id      text,
    error_message text,
    raw_bytes     bytea,
    details_json  jsonb,
    recorded_at   timestamptz NOT NULL DEFAULT now()
)
"""


async def bootstrap_dlq_tables(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
    """Create the DLQ fence + payload + ingest-reject tables if they don't exist.

    DSQL only accepts one DDL statement per transaction (`FeatureNotSupported: multiple
    ddl statements not supported in a transaction`), so each CREATE runs in its own
    implicit transaction. Idempotent — safe on every process start.
    """
    for ddl in (FENCES_DDL, PAYLOAD_DDL, INGEST_REJECTS_DDL):
        async with conn.cursor() as cur:
            await cur.execute(ddl)
        await conn.commit()
    log.info("dlq.schema.ready")
