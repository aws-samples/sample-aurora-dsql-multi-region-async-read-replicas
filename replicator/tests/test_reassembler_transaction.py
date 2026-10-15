"""Transaction reassembler tests.

Covers:
- Single-record transaction emits immediately on first ingest.
- Multi-record transaction emits only after the Nth record.
- Random-order arrival across multiple transactions produces complete and distinct outputs.
- Duplicate record_id within a transaction is deduped.
- Records arriving AFTER completion are ignored (don't resurrect the buffer).
- Deterministic intra-txn order: `records` emitted sorted by record_id.
- Mixed FullRecord + AssembledRecord in the same transaction.
- `sweep()` surfaces timed-out incomplete transactions.
"""

from __future__ import annotations

import random
from typing import Any

from dsql_replicator.envelope import FullRecord, parse
from dsql_replicator.reassembler import (
    AssembledRecord,
    CompletedTransaction,
    TransactionReassembler,
)


def _full(tx_id: str, record_id: str, total: int, ts_ns: int = 1) -> FullRecord:
    rec = parse(
        {
            "type": "full",
            "op": "c",
            "before": None,
            "after": {"rid": record_id},
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


def _assembled(tx_id: str, record_id: str, total: int, payload: Any = None) -> AssembledRecord:
    """Build an AssembledRecord by hand to simulate a chunked record emerging from fragment reassembly."""
    from dsql_replicator.envelope import ChunkedRecord

    rec = parse(
        {
            "type": "chunked",
            "op": "u",
            "before": None,
            "after": None,
            "source": {
                "version": "1.1",
                "ts_ms": 1,
                "ts_ns": 1,
                "txId": tx_id,
                "schema": "public",
                "table": "t",
                "db": "postgres",
                "cluster": "c",
            },
            "chunked": {"after": {"chunk_id": "cid", "total_fragments": 1, "crc32c": "0"}},
            "ts_ms": 1,
            "ts_ns": 1,
            "transaction": {"total_records": total, "record_id": record_id},
        }
    )
    assert isinstance(rec, ChunkedRecord)
    return AssembledRecord(main=rec, before=None, after=payload or {"big": "x" * 100})


# ---------- tests ----------


def test_single_record_transaction_emits_immediately() -> None:
    rs = TransactionReassembler()
    out = rs.ingest(_full("tx1", "r1", total=1))
    assert isinstance(out, CompletedTransaction)
    assert out.tx_id == "tx1"
    assert out.total_records == 1
    assert len(out.records) == 1
    assert rs.pending_transactions() == 0


def test_three_record_transaction_emits_on_third() -> None:
    rs = TransactionReassembler()
    assert rs.ingest(_full("tx2", "r-c", total=3)) is None
    assert rs.ingest(_full("tx2", "r-a", total=3)) is None
    out = rs.ingest(_full("tx2", "r-b", total=3))
    assert isinstance(out, CompletedTransaction)
    assert [_record_id_of(r) for r in out.records] == ["r-a", "r-b", "r-c"]  # sorted
    assert rs.pending_transactions() == 0


def test_duplicate_record_id_deduped() -> None:
    rs = TransactionReassembler()
    assert rs.ingest(_full("tx3", "r1", total=2)) is None
    assert rs.ingest(_full("tx3", "r1", total=2)) is None  # dup
    assert rs.ingest(_full("tx3", "r1", total=2)) is None  # dup again
    out = rs.ingest(_full("tx3", "r2", total=2))
    assert isinstance(out, CompletedTransaction)
    assert out.total_records == 2
    assert len(out.records) == 2


def test_late_duplicate_after_completion_is_ignored() -> None:
    rs = TransactionReassembler()
    rs.ingest(_full("tx4", "r1", total=1))  # completes immediately
    # A late replay of the same record must not create a second CompletedTransaction.
    out = rs.ingest(_full("tx4", "r1", total=1))
    assert out is None
    # And an unrelated record for a different tx_id still works.
    out2 = rs.ingest(_full("tx5", "r1", total=1))
    assert isinstance(out2, CompletedTransaction)


def test_random_order_1k_fuzz() -> None:
    """1000 transactions of 2-6 records each, randomized arrival across all events.

    Expect each tx_id to emit exactly once with the correct record set.
    """
    rng = random.Random(7)
    events: list[Any] = []
    expected: dict[str, set[str]] = {}

    for i in range(1000):
        tx_id = f"tx-{i}"
        n = rng.randint(2, 6)
        record_ids = [f"r-{i}-{j}" for j in range(n)]
        expected[tx_id] = set(record_ids)
        for rid in record_ids:
            events.append(_full(tx_id, rid, total=n, ts_ns=i * 1000 + rng.randint(0, 999)))

    rng.shuffle(events)
    rs = TransactionReassembler()
    emitted: dict[str, set[str]] = {}
    emitted_order: list[str] = []
    for ev in events:
        out = rs.ingest(ev)
        if out is not None:
            emitted_order.append(out.tx_id)
            emitted[out.tx_id] = {_record_id_of(r) for r in out.records}

    assert emitted == expected
    assert len(emitted_order) == 1000  # exactly one emission per tx_id
    assert len(set(emitted_order)) == 1000  # no duplicate emissions
    assert rs.pending_transactions() == 0


def test_mixed_full_and_assembled_in_one_txn() -> None:
    rs = TransactionReassembler()
    assert rs.ingest(_full("tx-mix", "r-full", total=2)) is None
    out = rs.ingest(_assembled("tx-mix", "r-chunk", total=2, payload={"heavy": "y" * 200}))
    assert isinstance(out, CompletedTransaction)
    assert len(out.records) == 2
    kinds = {type(r).__name__ for r in out.records}
    assert kinds == {"FullRecord", "AssembledRecord"}


def test_sweep_times_out_incomplete_transaction() -> None:
    rs = TransactionReassembler(timeout_seconds=1.0)
    rs.ingest(_full("tx-slow", "r1", total=3))
    rs.ingest(_full("tx-slow", "r2", total=3))
    assert rs.pending_transactions() == 1
    import time as _t

    future = _t.monotonic() + 2.0
    timeouts = rs.sweep(now_mono=future)
    assert len(timeouts) == 1
    assert timeouts[0].tx_id == "tx-slow"
    assert timeouts[0].buffered == 2
    assert timeouts[0].expected == 3
    assert rs.pending_transactions() == 0


def test_sweep_prevents_resurrection_of_timed_out_txn() -> None:
    """After a timeout, a late-arriving record for that txn must not create a new buffer."""
    rs = TransactionReassembler(timeout_seconds=1.0)
    rs.ingest(_full("tx-ghost", "r1", total=2))
    import time as _t

    future = _t.monotonic() + 2.0
    rs.sweep(now_mono=future)
    # Late third record arrives — must be ignored (treated as completed/dead).
    assert rs.ingest(_full("tx-ghost", "r2", total=2)) is None
    assert rs.pending_transactions() == 0


# ---------- helpers ----------


def _record_id_of(record: Any) -> str:
    if isinstance(record, AssembledRecord):
        return record.main.transaction.record_id
    return record.transaction.record_id  # type: ignore[union-attr]
