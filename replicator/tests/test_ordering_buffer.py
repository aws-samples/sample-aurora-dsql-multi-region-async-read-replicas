"""Ordering buffer tests.

Covers:
- Nothing ready when no shards registered or no shards reporting.
- Single-shard release: head.ts_ns strictly < W, equal-to-W held.
- Multi-shard watermark: W = min(shard latest).
- Lagging shard holds the pipeline; its advance unblocks releases.
- Idle shard with MillisBehindLatest == 0 → synthetic watermark advance drains the queue.
- Idle shard with MillisBehindLatest > 0 → stays frozen (does NOT advance synthetically).
- Fence gating: txns at or past fence_ts_ns held, below fence released.
- Dedup on re-ingest of the same tx_id.
- Backpressure: max_depth triggers BackpressureError.
- Determinism: ties broken by insertion order.
"""

from __future__ import annotations

import pytest

from dsql_replicator.envelope import FullRecord, parse
from dsql_replicator.ordering import OrderingBuffer
from dsql_replicator.ordering.buffer import BackpressureError
from dsql_replicator.reassembler import CompletedTransaction


def _full(tx_id: str, ts_ns: int) -> FullRecord:
    rec = parse(
        {
            "type": "full",
            "op": "c",
            "before": None,
            "after": {"x": 1},
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
            "transaction": {"total_records": 1, "record_id": tx_id},
        }
    )
    assert isinstance(rec, FullRecord)
    return rec


def _txn(tx_id: str, ts_ns: int) -> CompletedTransaction:
    return CompletedTransaction(tx_id=tx_id, ts_ns=ts_ns, total_records=1, records=(_full(tx_id, ts_ns),))


# ---------- tests ----------


def test_no_shards_no_release() -> None:
    ob = OrderingBuffer()
    ob.ingest(_txn("tx1", 100))
    assert ob.ready(now_mono=1000.0, now_wall_ns=1_000_000_000) == []


def test_single_shard_release_strictly_less_than_watermark() -> None:
    ob = OrderingBuffer()
    ob.register_shard("shard-0")
    ob.ingest(_txn("tx-50", 50))
    ob.ingest(_txn("tx-100", 100))
    ob.ingest(_txn("tx-150", 150))
    # Watermark at 100 → tx-50 is safe to release; tx-100 is NOT (not strictly less).
    ob.update_shard("shard-0", latest_ts_ns=100, millis_behind_latest=10)
    out = ob.ready(now_mono=1.0, now_wall_ns=1_000_000_000)
    assert [t.tx_id for t in out] == ["tx-50"]
    # Advance watermark to 200 → both tx-100 and tx-150 release.
    ob.update_shard("shard-0", latest_ts_ns=200, millis_behind_latest=5)
    out2 = ob.ready(now_mono=2.0, now_wall_ns=2_000_000_000)
    assert [t.tx_id for t in out2] == ["tx-100", "tx-150"]
    assert ob.depth() == 0


def test_multi_shard_watermark_is_min() -> None:
    ob = OrderingBuffer()
    ob.register_shard("shard-0")
    ob.register_shard("shard-1")
    ob.ingest(_txn("tx-50", 50))
    ob.ingest(_txn("tx-80", 80))
    ob.update_shard("shard-0", latest_ts_ns=200, millis_behind_latest=0)
    ob.update_shard("shard-1", latest_ts_ns=70, millis_behind_latest=100)
    # W = min(200, 70) = 70 → only tx-50 releases.
    out = ob.ready(now_mono=1.0, now_wall_ns=1_000_000_000)
    assert [t.tx_id for t in out] == ["tx-50"]
    assert ob.depth() == 1
    assert ob.watermark(now_mono=1.0, now_wall_ns=1_000_000_000) == 70


def test_idle_shard_with_zero_mbl_advances_synthetically() -> None:
    """An idle shard with MillisBehindLatest == 0 must not freeze the pipeline."""
    ob = OrderingBuffer(idle_shard_timeout_seconds=1.0, clock_skew_budget_seconds=0.001)
    ob.register_shard("busy")
    ob.register_shard("idle")
    # busy is at 10_000, idle is at 100 and never advances.
    ob.update_shard("busy", latest_ts_ns=10_000, millis_behind_latest=0, now_mono=0.0)
    ob.update_shard("idle", latest_ts_ns=100, millis_behind_latest=0, now_mono=0.0)
    ob.ingest(_txn("tx-50", 50))
    ob.ingest(_txn("tx-200", 200))
    ob.ingest(_txn("tx-5000", 5000))
    # Right at ingest time the idle shard caps W at 100 → only tx-50 releases.
    out1 = ob.ready(now_mono=0.1, now_wall_ns=100_000_000)
    assert [t.tx_id for t in out1] == ["tx-50"]
    # Advance monotonic past the idle timeout AND set wall-clock high enough that the
    # synthetic watermark exceeds all remaining txn ts_ns values.
    out2 = ob.ready(now_mono=10.0, now_wall_ns=10_000_000_000)
    assert [t.tx_id for t in out2] == ["tx-200", "tx-5000"]


def test_idle_shard_with_nonzero_mbl_does_not_advance_synthetically() -> None:
    """When MillisBehindLatest > 0, idle synthetic advance must NOT apply."""
    ob = OrderingBuffer(idle_shard_timeout_seconds=1.0)
    ob.register_shard("busy")
    ob.register_shard("backlogged")
    ob.update_shard("busy", latest_ts_ns=10_000, millis_behind_latest=0, now_mono=0.0)
    ob.update_shard("backlogged", latest_ts_ns=100, millis_behind_latest=5000, now_mono=0.0)
    ob.ingest(_txn("tx-50", 50))
    ob.ingest(_txn("tx-200", 200))
    out = ob.ready(now_mono=10.0, now_wall_ns=10_000_000_000)
    assert [t.tx_id for t in out] == ["tx-50"]  # 200 is above backlogged's 100 → held


def test_fence_holds_at_fence_ts() -> None:
    ob = OrderingBuffer()
    ob.register_shard("s0")
    ob.update_shard("s0", latest_ts_ns=10_000, millis_behind_latest=0)
    for tx_id, ts in [("a", 100), ("b", 500), ("c", 1000), ("d", 2000)]:
        ob.ingest(_txn(tx_id, ts))
    ob.set_fence(1000)
    out = ob.ready(now_mono=1.0, now_wall_ns=1_000_000_000)
    assert [t.tx_id for t in out] == ["a", "b"]  # c@1000 held by fence, d@2000 past fence
    # Closing the fence drains the rest.
    ob.set_fence(None)
    out2 = ob.ready(now_mono=1.0, now_wall_ns=1_000_000_000)
    assert [t.tx_id for t in out2] == ["c", "d"]


def test_dedup_on_reingest_same_tx_id() -> None:
    ob = OrderingBuffer()
    ob.register_shard("s0")
    ob.update_shard("s0", latest_ts_ns=10_000, millis_behind_latest=0)
    ob.ingest(_txn("tx1", 100))
    ob.ingest(_txn("tx1", 100))  # duplicate — should be ignored
    out = ob.ready(now_mono=1.0, now_wall_ns=1_000_000_000)
    assert len(out) == 1


def test_backpressure() -> None:
    ob = OrderingBuffer(max_depth=3)
    ob.ingest(_txn("a", 1))
    ob.ingest(_txn("b", 2))
    ob.ingest(_txn("c", 3))
    with pytest.raises(BackpressureError):
        ob.ingest(_txn("d", 4))


def test_order_is_ts_ns_ascending_with_stable_tiebreak() -> None:
    """Same ts_ns on multiple txns → emit in insertion order (sequence number tiebreak)."""
    ob = OrderingBuffer()
    ob.register_shard("s0")
    ob.update_shard("s0", latest_ts_ns=10_000, millis_behind_latest=0)
    for tx_id in ["first", "second", "third"]:
        ob.ingest(_txn(tx_id, 500))
    out = ob.ready(now_mono=1.0, now_wall_ns=1_000_000_000)
    assert [t.tx_id for t in out] == ["first", "second", "third"]
