"""Target-DSQL schema for the replicator state plane.

Two tables, idempotent `CREATE TABLE IF NOT EXISTS`. Each statement runs in its own
transaction because DSQL only allows one DDL per transaction
(`FeatureNotSupported: multiple ddl statements not supported in a transaction`).

- `__replicator_checkpoints`: `(stream_name, shard_id)` PK. `sequence_number` is the
  latest Kinesis sequence we have consumed for the shard. Advancement is in-band with
  apply when a shard contributed to the batch, plus a periodic best-effort flush for
  shards that are quiet (e.g., records parked in fragment reassembly). Row-level LWW
  makes re-apply idempotent on restart.

- `__replicator_shard_leases`: single-writer lease per (stream, shard). Steal pattern
  uses a conditional UPDATE; heartbeat uses the cached `lease_token` as a fencing
  token so a stale task can't accidentally refresh after losing the lease.
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

async def bootstrap_state_tables(conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
    """Create checkpoints + shard-leases tables. Idempotent.

    Each CREATE runs in its own commit (DSQL rejects multiple DDL per txn).
    Safe to call on every process start.
    """
    for ddl in (CHECKPOINTS_DDL, SHARD_LEASES_DDL):
        async with conn.cursor() as cur:
            await cur.execute(ddl)
        await conn.commit()
    log.info("state.schema.ready")
