"""Unit tests for CheckpointRepo and LeaseRepo using a FakeConn.

Full-SQL semantics are proved by the live-cluster failover test
(live-cluster harness). These tests pin down the Python-side behavior:
cursor sequencing, return values, exception handling.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg import errors as pg_errors

from dsql_replicator.state.checkpoint import CheckpointRepo
from dsql_replicator.state.lease import LeaseRepo


class _FakeCursor:
    """Minimal AsyncCursor stand-in. `responses` is a queue of (rowcount, rows) tuples."""

    def __init__(self, responses: list[tuple[int, list[tuple[Any, ...]]] | type[BaseException]]) -> None:
        self._responses = responses
        self._current: tuple[int, list[tuple[Any, ...]]] | None = None
        self.calls: list[tuple[str, Any]] = []

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def execute(self, sql: str, params: Any = None) -> None:
        self.calls.append((sql, params))
        if not self._responses:
            self._current = (0, [])
            return
        head = self._responses.pop(0)
        if isinstance(head, type) and issubclass(head, BaseException):
            raise head("injected")
        self._current = head

    @property
    def rowcount(self) -> int:
        assert self._current is not None
        return self._current[0]

    async def fetchone(self) -> tuple[Any, ...] | None:
        assert self._current is not None
        rows = self._current[1]
        return rows[0] if rows else None

    async def fetchall(self) -> list[tuple[Any, ...]]:
        assert self._current is not None
        return list(self._current[1])


class _FakeConn:
    def __init__(self, responses: list[Any]) -> None:
        self._cursor = _FakeCursor(responses)

    def cursor(self) -> _FakeCursor:
        return self._cursor


# ---------- CheckpointRepo ----------


@pytest.mark.asyncio
async def test_checkpoint_get_missing_returns_none() -> None:
    conn = _FakeConn([(0, [])])
    repo = CheckpointRepo(conn)  # type: ignore[arg-type]
    out = await repo.get("stream-A", "shard-0")
    assert out is None


@pytest.mark.asyncio
async def test_checkpoint_get_existing_parses_row() -> None:
    now = datetime(2026, 10, 12, 15, 30, 0)
    conn = _FakeConn(
        [
            (
                1,
                [
                    (
                        "stream-A",
                        "shard-0",
                        "AFTER_SEQUENCE_NUMBER",
                        "49651234...",
                        1_791_000_000_000_000_000,
                        now,
                    )
                ],
            )
        ]
    )
    repo = CheckpointRepo(conn)  # type: ignore[arg-type]
    out = await repo.get("stream-A", "shard-0")
    assert out is not None
    assert out.sequence_number == "49651234..."
    assert out.iterator_type == "AFTER_SEQUENCE_NUMBER"
    assert out.last_applied_ts_ns == 1_791_000_000_000_000_000


@pytest.mark.asyncio
async def test_checkpoint_upsert_emits_single_execute() -> None:
    cur = _FakeCursor([(1, [])])
    repo = CheckpointRepo(_FakeConn([]))  # type: ignore[arg-type]
    await repo.upsert_in_cursor(
        cur,  # type: ignore[arg-type]
        stream_name="s",
        shard_id="sh",
        sequence_number="42",
        last_applied_ts_ns=100,
    )
    assert len(cur.calls) == 1
    sql, params = cur.calls[0]
    assert "INSERT INTO __replicator_checkpoints" in sql
    assert params == ("s", "sh", "AFTER_SEQUENCE_NUMBER", "42", 100)


# ---------- LeaseRepo ----------


@pytest.mark.asyncio
async def test_lease_steal_from_expired_returns_new_token() -> None:
    token = uuid4()
    conn = _FakeConn([(1, [(token,)])])
    repo = LeaseRepo(conn)  # type: ignore[arg-type]
    out = await repo.steal(stream_name="s", shard_id="sh", task_id="t1", ttl_seconds=30)
    assert out == token


@pytest.mark.asyncio
async def test_lease_steal_inserts_when_missing_row() -> None:
    token = uuid4()
    conn = _FakeConn([(0, []), (1, [(token,)])])  # UPDATE returns 0 → INSERT succeeds
    repo = LeaseRepo(conn)  # type: ignore[arg-type]
    out = await repo.steal(stream_name="s", shard_id="sh", task_id="t1", ttl_seconds=30)
    assert out == token


@pytest.mark.asyncio
async def test_lease_steal_rejected_when_insert_conflicts() -> None:
    conn = _FakeConn([(0, []), pg_errors.UniqueViolation])
    repo = LeaseRepo(conn)  # type: ignore[arg-type]
    out = await repo.steal(stream_name="s", shard_id="sh", task_id="t1", ttl_seconds=30)
    assert out is None


@pytest.mark.asyncio
async def test_lease_heartbeat_true_when_rowcount_one() -> None:
    conn = _FakeConn([(1, [])])
    repo = LeaseRepo(conn)  # type: ignore[arg-type]
    ok = await repo.heartbeat(
        stream_name="s",
        shard_id="sh",
        task_id="t1",
        lease_token=uuid4(),
        ttl_seconds=30,
    )
    assert ok is True


@pytest.mark.asyncio
async def test_lease_heartbeat_false_when_token_mismatch() -> None:
    conn = _FakeConn([(0, [])])
    repo = LeaseRepo(conn)  # type: ignore[arg-type]
    ok = await repo.heartbeat(
        stream_name="s",
        shard_id="sh",
        task_id="t1",
        lease_token=uuid4(),
        ttl_seconds=30,
    )
    assert ok is False


@pytest.mark.asyncio
async def test_lease_release_true_when_owned() -> None:
    conn = _FakeConn([(1, [])])
    repo = LeaseRepo(conn)  # type: ignore[arg-type]
    ok = await repo.release(
        stream_name="s",
        shard_id="sh",
        task_id="t1",
        lease_token=UUID("00000000-0000-0000-0000-000000000001"),
    )
    assert ok is True


@pytest.mark.asyncio
async def test_lease_list_parses_rows() -> None:
    now = datetime(2026, 10, 12, 10, 0, 0)
    token = uuid4()
    conn = _FakeConn([(2, [("s", "sh", "t1", token, now, now)])])
    repo = LeaseRepo(conn)  # type: ignore[arg-type]
    rows = await repo.list_leases()
    assert len(rows) == 1
    assert rows[0].task_id == "t1"
    assert rows[0].lease_token == token
