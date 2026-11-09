"""Replicator state plane — target-DSQL tables for checkpoints + shard leases.

Scope (post-2026-11 simplification pass):
- `__replicator_checkpoints` — iterator position per (stream, shard).
- `__replicator_shard_leases` — single-writer lease per (stream, shard) with
  heartbeats and expiry.

The old `__replicator_watermark_state` table and the in-process
`ShardSeqTracker` were dropped alongside the ordering buffer; checkpoints
are now best-effort per-shard flushes and row-level LWW makes re-apply
idempotent on restart.
"""

from __future__ import annotations

from .checkpoint import CheckpointRepo, CheckpointRow
from .lease import LeaseRepo, LeaseRow
from .schema import (
    CHECKPOINTS_DDL,
    SHARD_LEASES_DDL,
    bootstrap_state_tables,
)

__all__ = [
    "CHECKPOINTS_DDL",
    "SHARD_LEASES_DDL",
    "CheckpointRepo",
    "CheckpointRow",
    "LeaseRepo",
    "LeaseRow",
    "bootstrap_state_tables",
]
