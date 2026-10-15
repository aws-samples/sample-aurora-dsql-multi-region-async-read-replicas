"""Checkpoint repository: per-shard iterator position in target DSQL.

Stores the Kinesis sequence number of the last applied-or-DLQ'd record per
`(stream_name, shard_id)`. Checkpoint advance is **atomic with the row apply**: the
Applier's `pre_commit_hook` calls `CheckpointRepo.upsert_in_cursor` inside the same
DSQL transaction as the row inserts + idempotency stamps + fence close. If any step
fails, nothing commits.

Restart recovery: a shard worker reads its checkpoint on startup and uses
`AFTER_SEQUENCE_NUMBER` iterator type with the stored sequence; absent a checkpoint,
it falls back to `LATEST` (first-run) or `TRIM_HORIZON` depending on operator intent.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg


@dataclass(frozen=True)
class CheckpointRow:
    stream_name: str
    shard_id: str
    iterator_type: str
    sequence_number: str
    last_applied_ts_ns: int | None
    updated_at: datetime


class CheckpointRepo:
    """Reads + writes `__replicator_checkpoints`."""

    def __init__(self, conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
        self._conn = conn

    async def get(self, stream_name: str, shard_id: str) -> CheckpointRow | None:
        async with self._conn.cursor() as cur:
            await cur.execute(
                "SELECT stream_name, shard_id, iterator_type, sequence_number, "
                "       last_applied_ts_ns, updated_at "
                "FROM __replicator_checkpoints "
                "WHERE stream_name = %s AND shard_id = %s",
                (stream_name, shard_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return CheckpointRow(
            stream_name=str(row[0]),
            shard_id=str(row[1]),
            iterator_type=str(row[2]),
            sequence_number=str(row[3]),
            last_applied_ts_ns=None if row[4] is None else int(row[4]),  # type: ignore[call-overload]
            updated_at=row[5],  # type: ignore[arg-type]
        )

    async def upsert_in_cursor(
        self,
        cur: psycopg.AsyncCursor[Any],
        *,
        stream_name: str,
        shard_id: str,
        sequence_number: str,
        last_applied_ts_ns: int,
        iterator_type: str = "AFTER_SEQUENCE_NUMBER",
    ) -> None:
        """Upsert checkpoint inside the caller's cursor (same txn as row apply + stamp).

        Only advances the checkpoint — `sequence_number` is overwritten unconditionally
        because Kinesis sequence numbers are per-shard-monotonic strings, and the caller
        guarantees we only call this for an in-order apply.
        """
        await cur.execute(
            "INSERT INTO __replicator_checkpoints "
            "(stream_name, shard_id, iterator_type, sequence_number, last_applied_ts_ns, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, now()) "
            "ON CONFLICT (stream_name, shard_id) DO UPDATE SET "
            "  iterator_type      = EXCLUDED.iterator_type, "
            "  sequence_number    = EXCLUDED.sequence_number, "
            "  last_applied_ts_ns = EXCLUDED.last_applied_ts_ns, "
            "  updated_at         = now()",
            (stream_name, shard_id, iterator_type, sequence_number, last_applied_ts_ns),
        )
