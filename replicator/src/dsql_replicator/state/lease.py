"""Shard-lease repository: single-writer coordination for (stream_name, shard_id).

Backs `__replicator_shard_leases`. The primitives are plain SQL — DSQL supports the
exact atomic conditional updates we need, no advisory locks required.

Steal pattern (atomic acquire-or-reject in one SQL statement):

    UPDATE __replicator_shard_leases
       SET task_id      = $self,
           lease_token  = gen_random_uuid(),
           expires_at   = now() + $ttl,
           heartbeat_at = now()
     WHERE (stream_name, shard_id) = ($s, $sh)
       AND (task_id = $self OR expires_at < now())
    RETURNING lease_token;

If `rowcount = 0` the row either doesn't exist or someone else owns a live lease. On
missing-row we INSERT; on live-lease we return None.

Heartbeat pattern (fencing token):

    UPDATE __replicator_shard_leases
       SET expires_at   = now() + $ttl,
           heartbeat_at = now()
     WHERE (stream_name, shard_id) = ($s, $sh)
       AND task_id    = $self
       AND lease_token = $cached_token
    RETURNING lease_token;

If `rowcount = 0` the cached token no longer matches — we lost the lease and must
surrender the shard. The fencing token prevents a paused-then-resumed task from
accidentally refreshing a lease that was stolen while it was paused.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import psycopg


@dataclass(frozen=True)
class LeaseRow:
    stream_name: str
    shard_id: str
    task_id: str
    lease_token: UUID
    expires_at: datetime
    heartbeat_at: datetime


class LeaseRepo:
    """Reads + writes `__replicator_shard_leases`."""

    def __init__(self, conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
        self._conn = conn

    async def list_leases(self) -> list[LeaseRow]:
        async with self._conn.cursor() as cur:
            await cur.execute(
                "SELECT stream_name, shard_id, task_id, lease_token, expires_at, heartbeat_at "
                "FROM __replicator_shard_leases "
                "ORDER BY stream_name, shard_id"
            )
            rows = await cur.fetchall()
        return [_row_to_lease(r) for r in rows]

    async def get(self, stream_name: str, shard_id: str) -> LeaseRow | None:
        async with self._conn.cursor() as cur:
            await cur.execute(
                "SELECT stream_name, shard_id, task_id, lease_token, expires_at, heartbeat_at "
                "FROM __replicator_shard_leases "
                "WHERE stream_name = %s AND shard_id = %s",
                (stream_name, shard_id),
            )
            row = await cur.fetchone()
        return None if row is None else _row_to_lease(row)

    async def steal(self, *, stream_name: str, shard_id: str, task_id: str, ttl_seconds: int) -> UUID | None:
        """Try to acquire the lease. Returns new `lease_token` on success, None if
        someone else holds a live lease. Caller MUST cache the returned token for
        future heartbeats and release calls.
        """
        async with self._conn.cursor() as cur:
            await cur.execute(
                "UPDATE __replicator_shard_leases "
                "SET task_id = %s, lease_token = gen_random_uuid(), "
                "    expires_at = now() + make_interval(secs => %s), "
                "    heartbeat_at = now() "
                "WHERE stream_name = %s AND shard_id = %s "
                "  AND (task_id = %s OR expires_at < now()) "
                "RETURNING lease_token",
                (task_id, ttl_seconds, stream_name, shard_id, task_id),
            )
            row = await cur.fetchone()
            if row is not None:
                return _coerce_uuid(row[0])
            # No row updated — either someone else owns a live lease OR row doesn't exist.
            # Try to INSERT (DSQL will reject if the PK is taken).
            try:
                await cur.execute(
                    "INSERT INTO __replicator_shard_leases "
                    "(stream_name, shard_id, task_id, lease_token, expires_at) "
                    "VALUES (%s, %s, %s, gen_random_uuid(), now() + make_interval(secs => %s)) "
                    "RETURNING lease_token",
                    (stream_name, shard_id, task_id, ttl_seconds),
                )
                row = await cur.fetchone()
                return None if row is None else _coerce_uuid(row[0])
            except psycopg.errors.UniqueViolation:
                return None

    async def heartbeat(
        self,
        *,
        stream_name: str,
        shard_id: str,
        task_id: str,
        lease_token: UUID,
        ttl_seconds: int,
    ) -> bool:
        """Refresh an owned lease. Returns False if the fencing token no longer matches
        (lease was stolen) — caller MUST surrender the shard immediately.
        """
        async with self._conn.cursor() as cur:
            await cur.execute(
                "UPDATE __replicator_shard_leases "
                "SET expires_at = now() + make_interval(secs => %s), heartbeat_at = now() "
                "WHERE stream_name = %s AND shard_id = %s "
                "  AND task_id = %s AND lease_token = %s",
                (ttl_seconds, stream_name, shard_id, task_id, str(lease_token)),
            )
            return int(cur.rowcount) == 1

    async def release(
        self,
        *,
        stream_name: str,
        shard_id: str,
        task_id: str,
        lease_token: UUID,
    ) -> bool:
        """Voluntarily surrender the lease (clean shutdown). Returns False if we already
        lost it. Non-fatal — caller shuts down regardless."""
        async with self._conn.cursor() as cur:
            await cur.execute(
                "UPDATE __replicator_shard_leases "
                "SET expires_at = now() "
                "WHERE stream_name = %s AND shard_id = %s "
                "  AND task_id = %s AND lease_token = %s",
                (stream_name, shard_id, task_id, str(lease_token)),
            )
            return int(cur.rowcount) == 1


# ---- helpers --------------------------------------------------------------


def _coerce_uuid(v: Any) -> UUID:
    return v if isinstance(v, UUID) else UUID(str(v))


def _row_to_lease(row: Any) -> LeaseRow:
    return LeaseRow(
        stream_name=str(row[0]),
        shard_id=str(row[1]),
        task_id=str(row[2]),
        lease_token=_coerce_uuid(row[3]),
        expires_at=row[4],
        heartbeat_at=row[5],
    )
