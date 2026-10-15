"""Replicator state plane — target-DSQL tables for checkpoints, shard leases, watermarks.

Scope (M4+):
- `__replicator_checkpoints` — iterator position + last applied ts per (stream, shard).
- `__replicator_shard_leases` — single-writer lease per (stream, shard) with heartbeats
  and expiry. Replaces the DDB-based model from the original spec.
- `__replicator_watermark_state` — persisted mirror of per-shard watermarks for cold
  restart (so a new task doesn't start from `latest_ts_ns=0` while others have
  already advanced).

All three live in the target DSQL cluster, parallel to the 2026-10-09 idempotency
decision and the 2026-10-12 DLQ-in-DSQL decision.
"""

from __future__ import annotations

from .checkpoint import CheckpointRepo, CheckpointRow
from .lease import LeaseRepo, LeaseRow
from .schema import (
    CHECKPOINTS_DDL,
    SHARD_LEASES_DDL,
    WATERMARK_STATE_DDL,
    bootstrap_state_tables,
)

__all__ = [
    "CHECKPOINTS_DDL",
    "SHARD_LEASES_DDL",
    "WATERMARK_STATE_DDL",
    "CheckpointRepo",
    "CheckpointRow",
    "LeaseRepo",
    "LeaseRow",
    "bootstrap_state_tables",
]
