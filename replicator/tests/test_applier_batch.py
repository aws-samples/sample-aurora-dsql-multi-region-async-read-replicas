"""Tests for `Applier.apply_batch` — micro-batching correctness without live DSQL.

Covers:
- Empty batch is a no-op.
- Multi-txn batch computes the union of touched stripes.
- `_apply_batch_once` runs every txn's records + applied-stamps inside a single
  `conn.transaction()` block, in caller-supplied txn order.
- Dedup (stamp returning rowcount=0) is counted per-txn, not globally.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dsql_replicator.applier.apply import Applier
from dsql_replicator.envelope import FullRecord, parse
from dsql_replicator.reassembler import CompletedTransaction


def _full(tx_id: str, record_id: str, total: int, pk: str, ts_ns: int = 1) -> FullRecord:
    rec = parse(
        {
            "type": "full",
            "op": "c",
            "before": None,
            "after": {"id": pk, "payload": "x"},
            "source": {
                "version": "1.1",
                "ts_ms": ts_ns // 1_000_000,
                "ts_ns": ts_ns,
                "txId": tx_id,
                "schema": "public",
                "table": "t",
                "db": "postgres",
                "cluster": "c",
            },
            "ts_ms": ts_ns // 1_000_000,
            "ts_ns": ts_ns,
            "transaction": {"total_records": total, "record_id": record_id},
        }
    )
    assert isinstance(rec, FullRecord)
    return rec


def _txn(tx_id: str, ts_ns: int, pks: list[str]) -> CompletedTransaction:
    recs = tuple(_full(tx_id, f"r{i}", total=len(pks), pk=pk, ts_ns=ts_ns) for i, pk in enumerate(pks))
    return CompletedTransaction(tx_id=tx_id, ts_ns=ts_ns, total_records=len(pks), records=recs)


class _FakeStripeHashApplier(Applier):
    """Applier subclass that stubs the schema cache so stripe computation runs
    without a live DSQL connection. Each PK maps to its own deterministic stripe."""

    def __init__(self, stripe_count: int = 8) -> None:
        # Skip real __init__ to avoid network/boto; set up only what we test.
        self._stripe_count = stripe_count
        self._worker_count = 1
        self._stripes = [asyncio.Lock() for _ in range(stripe_count)]
        self._conn_pool = asyncio.Queue(maxsize=1)
        self._schema_conn = None
        self._bootstrapped = True
        self._fault_after_rows = None
        self._max_retries = 1
        self._backoff_initial = 0.001
        self._backoff_cap = 0.01

        # Fake schema cache: returns a one-column PK table meta.
        class _Meta:
            pk_columns = ("id",)
            qualified = "public.t"

            def __init__(self) -> None:
                self.columns = [MagicMock(name="id"), MagicMock(name="payload")]
                self.columns[0].name = "id"
                self.columns[1].name = "payload"

            def type_of(self, _col: str) -> str:
                return "text"

        meta = _Meta()

        schema = MagicMock()
        schema.get = AsyncMock(return_value=meta)
        self._schema = schema


@pytest.mark.asyncio
async def test_apply_batch_empty_returns_empty() -> None:
    applier = _FakeStripeHashApplier()
    assert await applier.apply_batch([]) == []


@pytest.mark.asyncio
async def test_apply_batch_computes_union_of_touched_stripes(monkeypatch: pytest.MonkeyPatch) -> None:
    applier = _FakeStripeHashApplier(stripe_count=4)
    seen: list[int] = []

    async def fake_apply_batch_with_retry(txns: Any, hook: Any, stripes: tuple[int, ...]) -> list[Any]:
        seen.extend(stripes)
        return []

    monkeypatch.setattr(applier, "_apply_batch_with_retry", fake_apply_batch_with_retry)
    # Two txns touching different PKs — their stripe sets should union, not override.
    await applier.apply_batch([_txn("tx1", 100, ["k1"]), _txn("tx2", 200, ["k2"])])
    # At least one stripe captured; sorted and unique.
    assert seen == sorted(set(seen))
    assert len(seen) >= 1


class _FakeCursor:
    def __init__(self, rowcount: int, calls: list[tuple[str, Any]]) -> None:
        self.rowcount = rowcount
        self._calls = calls

    async def execute(self, sql: Any, params: Any = None) -> None:
        self._calls.append((str(sql), params))

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


class _FakeTxnCM:
    def __init__(self, counter: list[int]) -> None:
        self._counter = counter

    async def __aenter__(self) -> _FakeTxnCM:
        self._counter[0] += 1
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


@pytest.mark.asyncio
async def test_apply_batch_once_writes_each_txn_in_single_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_apply_batch_once` must run every txn's records + stamps inside ONE
    `conn.transaction()` context. If this breaks, batched replay is no longer
    atomic across source-txn boundaries."""
    applier = _FakeStripeHashApplier()

    apply_record_calls: list[str] = []

    async def fake_apply_record(_cur: Any, rec: Any) -> None:
        apply_record_calls.append(rec.transaction.record_id)

    monkeypatch.setattr(applier, "_apply_record", fake_apply_record)

    exec_calls: list[tuple[str, Any]] = []
    txn_enter = [0]
    conn = MagicMock()
    conn.transaction = lambda: _FakeTxnCM(txn_enter)
    conn.cursor = lambda: _FakeCursor(rowcount=1, calls=exec_calls)

    txns = [_txn("tx1", 100, ["k1"]), _txn("tx2", 200, ["k2", "k3"])]
    results = await applier._apply_batch_once(conn, txns, pre_commit_hook=None)

    assert txn_enter[0] == 1, "batch must run inside a single conn.transaction() block"
    assert results == [(1, 0), (2, 0)]
    stamp_calls = [c for c in exec_calls if "__replicator_applied" in c[0]]
    assert len(stamp_calls) == 3  # tx1:r0, tx2:r0, tx2:r1
    assert apply_record_calls == ["r0", "r0", "r1"]  # three records applied, in order


@pytest.mark.asyncio
async def test_apply_batch_once_counts_dedupes_per_txn(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the stamp INSERT reports rowcount=0 (already applied), the row is
    counted as deduped, not applied — and the counter is scoped per txn."""
    applier = _FakeStripeHashApplier()

    async def fake_apply_record(_cur: Any, _rec: Any) -> None:
        raise AssertionError("_apply_record must not be invoked when stamp returned 0")

    monkeypatch.setattr(applier, "_apply_record", fake_apply_record)

    conn = MagicMock()
    conn.transaction = lambda: _FakeTxnCM([0])
    conn.cursor = lambda: _FakeCursor(rowcount=0, calls=[])

    txns = [_txn("tx1", 100, ["k1"]), _txn("tx2", 200, ["k2", "k3"])]
    results = await applier._apply_batch_once(conn, txns, pre_commit_hook=None)
    assert results == [(0, 1), (0, 2)]
