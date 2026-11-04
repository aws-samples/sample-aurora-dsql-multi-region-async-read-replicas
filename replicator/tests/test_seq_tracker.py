"""Per-shard Kinesis-sequence tracker (review issue #1).

The tracker is the mechanism that lets `_make_checkpoint_hook` compute a
"safe-to-persist" seq per shard — the max seq such that every lower seq has
been applied-or-rejected. Previously, a timer-based flush could checkpoint
past still-buffered records and lose them on crash.

Covered:
- enroll + retire by (tx_id, record_id).
- enroll + retire by chunk_id.
- enroll + retire by seq (parse-error path).
- Safe-checkpoint is None when nothing is in-flight AND nothing is consumed.
- Safe-checkpoint equals max-consumed when every record retired.
- Safe-checkpoint is strictly less than min(in-flight) when something is buffered.
- Safe-checkpoint never regresses even if higher seqs retire first.
- Cross-shard isolation: retiring on A doesn't advance B.
"""

from __future__ import annotations

from dsql_replicator.state import ShardSeqTracker


def test_empty_tracker_returns_no_safe_checkpoint() -> None:
    t = ShardSeqTracker()
    assert t.safe_checkpoint("shard-A") is None


def test_single_record_enroll_then_retire_advances_checkpoint() -> None:
    t = ShardSeqTracker()
    t.enroll(shard_id="A", seq_str="100", tx_id="tx1", record_id="r1")
    # Before retire: nothing is safe to persist.
    assert t.safe_checkpoint("A") is None
    touched = t.retire_by_tx_record("tx1", "r1")
    assert touched == {"A"}
    safe = t.safe_checkpoint("A")
    assert safe is not None
    assert safe[0] == "100"


def test_safe_checkpoint_is_strictly_less_than_min_in_flight() -> None:
    """Shard A: enroll seqs 100, 200, 300. Retire 100 and 300. Nothing earlier
    than 200 is in-flight — but 300 is ahead of 200. Safe = 100 (the only
    retired seq strictly below min-in-flight=200)."""
    t = ShardSeqTracker()
    t.enroll(shard_id="A", seq_str="100", tx_id="tx1", record_id="r1")
    t.enroll(shard_id="A", seq_str="200", tx_id="tx2", record_id="r2")
    t.enroll(shard_id="A", seq_str="300", tx_id="tx3", record_id="r3")

    t.retire_by_tx_record("tx1", "r1")  # retires 100
    t.retire_by_tx_record("tx3", "r3")  # retires 300 (out of order)
    safe = t.safe_checkpoint("A")
    assert safe is not None
    assert safe[0] == "100"


def test_safe_checkpoint_advances_to_max_consumed_when_all_retired() -> None:
    t = ShardSeqTracker()
    t.enroll(shard_id="A", seq_str="100", tx_id="tx1", record_id="r1")
    t.enroll(shard_id="A", seq_str="200", tx_id="tx2", record_id="r2")
    t.retire_by_tx_record("tx1", "r1")
    t.retire_by_tx_record("tx2", "r2")
    safe = t.safe_checkpoint("A")
    assert safe is not None
    assert safe[0] == "200"


def test_safe_checkpoint_never_regresses() -> None:
    """Calling safe_checkpoint multiple times yields None once we've already
    reported the current high-water. This keeps the pre_commit_hook from
    emitting redundant upserts."""
    t = ShardSeqTracker()
    t.enroll(shard_id="A", seq_str="100", tx_id="tx1", record_id="r1")
    t.retire_by_tx_record("tx1", "r1")
    first = t.safe_checkpoint("A")
    assert first is not None
    assert first[0] == "100"
    # Second call: no new progress → None.
    assert t.safe_checkpoint("A") is None


def test_retire_by_chunk_covers_fragment_only_records() -> None:
    """Fragments carry no tx_id/record_id — they're enrolled by chunk_id and
    must retire by chunk_id when the AssembledRecord commits."""
    t = ShardSeqTracker()
    t.enroll(shard_id="A", seq_str="100", chunk_ids=("cid-1",))
    t.enroll(shard_id="A", seq_str="101", chunk_ids=("cid-1",))
    assert t.safe_checkpoint("A") is None
    touched = t.retire_by_chunk("cid-1")
    assert touched == {"A"}
    safe = t.safe_checkpoint("A")
    assert safe is not None
    assert safe[0] == "101"


def test_retire_by_seq_handles_parse_errors() -> None:
    """A parse error has no tx_id or chunk_id — only the raw Kinesis seq."""
    t = ShardSeqTracker()
    t.enroll(shard_id="A", seq_str="100")
    t.retire_by_seq("A", "100")
    safe = t.safe_checkpoint("A")
    assert safe is not None
    assert safe[0] == "100"


def test_cross_shard_isolation() -> None:
    t = ShardSeqTracker()
    t.enroll(shard_id="A", seq_str="100", tx_id="tx1", record_id="r1")
    t.enroll(shard_id="B", seq_str="500", tx_id="tx2", record_id="r2")
    t.retire_by_tx_record("tx1", "r1")
    safe_a = t.safe_checkpoint("A")
    safe_b = t.safe_checkpoint("B")
    assert safe_a is not None
    assert safe_a[0] == "100"
    assert safe_b is None  # B still has 500 in-flight


def test_blocking_record_prevents_checkpoint_advance() -> None:
    """The whole point: if a lower seq is still buffered when a higher seq
    applies, we MUST NOT advance the checkpoint past the buffered record."""
    t = ShardSeqTracker()
    t.enroll(shard_id="A", seq_str="100", tx_id="tx-buffered", record_id="r0")  # still in reassembly
    t.enroll(shard_id="A", seq_str="200", tx_id="tx-fast", record_id="r1")  # completes first
    t.retire_by_tx_record("tx-fast", "r1")
    # 100 is still in-flight → safe checkpoint must be None (nothing is strictly
    # safe before 200 since 100 is still unretired).
    assert t.safe_checkpoint("A") is None


def test_forget_shard_drops_state() -> None:
    t = ShardSeqTracker()
    t.enroll(shard_id="A", seq_str="100", tx_id="tx1", record_id="r1")
    t.forget_shard("A")
    assert t.safe_checkpoint("A") is None
    assert t.in_flight_count("A") == 0


def test_mixed_tx_and_chunk_retire_from_one_commit() -> None:
    """A chunked record contributes to BOTH `(tx_id, record_id)` (the main) and
    `chunk_id` (the fragments). The pre_commit_hook retires both forms."""
    t = ShardSeqTracker()
    # Main record (ChunkedRecord) on shard A.
    t.enroll(
        shard_id="A",
        seq_str="100",
        tx_id="tx1",
        record_id="r1",
        chunk_ids=("cid-after",),
    )
    # Fragments for the same chunk on shard B.
    t.enroll(shard_id="B", seq_str="200", chunk_ids=("cid-after",))
    t.enroll(shard_id="B", seq_str="201", chunk_ids=("cid-after",))
    # The CompletedTransaction commits.
    touched = t.retire_by_tx_record("tx1", "r1") | t.retire_by_chunk("cid-after")
    assert touched == {"A", "B"}
    assert t.safe_checkpoint("A") is not None
    assert t.safe_checkpoint("B") is not None
