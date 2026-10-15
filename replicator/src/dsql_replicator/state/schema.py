"""Target-DSQL schema for the replicator state plane (M4+).

Three tables, idempotent `CREATE TABLE IF NOT EXISTS`. Each statement runs in its own
transaction because DSQL only allows one DDL per transaction
(`FeatureNotSupported: multiple ddl statements not supported in a transaction`).

Design notes:

- `__replicator_checkpoints`: `(stream_name, shard_id)` PK. `sequence_number` is the
  last Kinesis sequence that we've either applied-or-DLQ'd for this shard. Checkpoint
  advancement must be atomic with the row apply + stamp — do the UPDATE inside the
  same transaction as the row inserts (same pattern as the DLQ fence close).

- `__replicator_shard_leases`: single-writer lease per (stream, shard). Steal pattern:
    `UPDATE … SET task_id = :self, lease_token = :new_uuid, expires_at = now() + :ttl
     WHERE (stream_name, shard_id) = (:s, :sh)
       AND (task_id = :self OR expires_at < now())
     RETURNING lease_token;`
  If `rowcount == 0`, someone else owns the lease. Heartbeat pattern uses the cached
  `lease_token` as a fencing token so a stale task can't accidentally refresh after
  losing the lease.

- `__replicator_watermark_state`: persisted mirror of per-shard watermarks. On cold
  start a worker reads this to seed its in-memory watermark; the ordering buffer gate
  then uses the live `min(effective_ts_ns)` across all registered shards. Not required
  for correctness — in-memory watermark converges within one poll — but shortens the
  warm-up window where no commits advance.
"""

from __future__ import annotations

import psycopg
import structlog

log = structlog.get_logger("state.schema")

CHECKPOINTS_DDL = """
CREATE TABLE IF NOT EXISTS __replicator_checkpoints (
    stream_name        text        NOT NULL,
    shard_id           text        NOT NULL,
    iterator_type      text        NOT NULL,
    sequence_number    text        NOT NULL,
    last_applied_ts_ns bigint,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_name, shard_id)
)
"""

SHARD_LEASES_DDL = """
CREATE TABLE IF NOT EXISTS __replicator_shard_leases (
    stream_name   text        NOT NULL,
    shard_id      text        NOT NULL,
    task_id       text        NOT NULL,
    lease_token   uuid        NOT NULL,
    expires_at    timestamptz NOT NULL,
    heartbeat_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_name, shard_id)
)
"""

WATERMARK_STATE_DDL = """
CREATE TABLE IF NOT EXISTS __replicator_watermark_state (
    stream_name          text        NOT NULL,
    shard_id             text        NOT NULL,
    latest_ts_ns         bigint      NOT NULL,
    millis_behind_latest integer     NOT NULL,
    updated_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_name, shard_id)
)
"""


async def bootstrap_state_tables(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
    """Create checkpoints + shard-leases + watermark-state tables. Idempotent.

    Each CREATE runs in its own commit (DSQL rejects multiple DDL per txn).
    Safe to call on every process start.
    """
    for ddl in (CHECKPOINTS_DDL, SHARD_LEASES_DDL, WATERMARK_STATE_DDL):
        async with conn.cursor() as cur:
            await cur.execute(ddl)
        await conn.commit()
    log.info("state.schema.ready")
