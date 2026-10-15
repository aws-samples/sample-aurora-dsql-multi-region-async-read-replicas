"""DLQ fence subsystem.

State plane lives in the TARGET DSQL cluster (parallel to the 2026-10-09 idempotency
decision). Two tables:

- `__replicator_dlq_fences(tx_id, fence_ts_ns, opened_at, closed_at, status, error_class, failing_sql)`
- `__replicator_dlq_payload(tx_id, payload_json jsonb)`

The fence row is the gating artifact: when a fence row has `status = 'OPEN'`, the live
consumer sets `OrderingBuffer.fence_ts_ns = fence_ts_ns` and the applier halts all
commits past that timestamp. SNS fires `DLQNewFence` as a side-effect notification.
"""

from __future__ import annotations

from .schema import FENCES_DDL, PAYLOAD_DDL, bootstrap_dlq_tables
from .writer import DlqWriter, FenceStatus, OpenFenceRow, deserialize_txn

__all__ = [
    "FENCES_DDL",
    "PAYLOAD_DDL",
    "DlqWriter",
    "FenceStatus",
    "OpenFenceRow",
    "bootstrap_dlq_tables",
    "deserialize_txn",
]
