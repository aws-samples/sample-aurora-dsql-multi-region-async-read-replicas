"""Fragment reassembler tests.

Covers:
- Full-record passthrough.
- Chunked record whose fragments all arrive BEFORE the main record (orphan fragments).
- Chunked record whose fragments arrive AFTER the main record.
- Interleaved order: fragment, fragment, main, fragment.
- Fragments split across before AND after images on the same record.
- Duplicate fragment delivery is deduped.
- CRC32C mismatch raises `CrcMismatchError`.
- Timeout: an incomplete main or orphan chunks are surfaced by `sweep()`.
"""

from __future__ import annotations

import json
import random
from typing import Any

import pytest
from crc32c import crc32c as _crc32c

from dsql_replicator.envelope import ChunkedRecord, FragmentRecord, FullRecord, parse
from dsql_replicator.reassembler import (
    AssembledRecord,
    CrcMismatchError,
    FragmentReassembler,
)


def _crc(text: str) -> str:
    return str(_crc32c(text.encode("utf-8")))


def _split_text(text: str, parts: int) -> list[str]:
    # Split by equal character counts on UTF-8 boundaries; our test strings are ASCII.
    step = max(1, len(text) // parts)
    chunks = [text[i : i + step] for i in range(0, len(text), step)]
    while len(chunks) < parts:
        chunks.append("")
    # Combine trailing extras if we overshot.
    if len(chunks) > parts:
        chunks[parts - 1] = "".join(chunks[parts - 1 :])
        chunks = chunks[:parts]
    assert "".join(chunks) == text
    return chunks


def _full_record(tx_id: str = "tx1") -> FullRecord:
    rec = parse(
        {
            "type": "full",
            "op": "c",
            "before": None,
            "after": {"x": 1},
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
            "ts_ms": 1,
            "ts_ns": 1,
            "transaction": {"total_records": 1, "record_id": "r"},
        }
    )
    assert isinstance(rec, FullRecord)
    return rec


def _chunked_main(
    tx_id: str,
    record_id: str,
    *,
    before: tuple[str, int, str] | None = None,  # (chunk_id, total_fragments, crc32c)
    after: tuple[str, int, str] | None = None,
) -> ChunkedRecord:
    chunked: dict[str, dict[str, Any]] = {}
    if before:
        chunked["before"] = {
            "chunk_id": before[0],
            "total_fragments": before[1],
            "crc32c": before[2],
        }
    if after:
        chunked["after"] = {"chunk_id": after[0], "total_fragments": after[1], "crc32c": after[2]}
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
            "chunked": chunked,
            "ts_ms": 1,
            "ts_ns": 1,
            "transaction": {"total_records": 1, "record_id": record_id},
        }
    )
    assert isinstance(rec, ChunkedRecord)
    return rec


def _fragment(chunk_id: str, index: int, data: str) -> FragmentRecord:
    rec = parse({"type": "fragment", "chunk_id": chunk_id, "index": index, "data": data})
    assert isinstance(rec, FragmentRecord)
    return rec


# ---------- tests ----------


def test_full_record_passes_through() -> None:
    rs = FragmentReassembler()
    full = _full_record()
    out = rs.ingest(full)
    assert out is full
    assert rs.pending_mains() == 0
    assert rs.pending_chunks() == 0


def test_main_first_then_fragments_in_order() -> None:
    rs = FragmentReassembler()
    payload = {"order_id": 1001, "payload": "x" * 200}
    text = json.dumps(payload)
    parts = _split_text(text, 3)
    main = _chunked_main("tx1", "r1", after=("c-after", 3, _crc(text)))
    assert rs.ingest(main) is None
    assert rs.ingest(_fragment("c-after", 0, parts[0])) is None
    assert rs.ingest(_fragment("c-after", 1, parts[1])) is None
    out = rs.ingest(_fragment("c-after", 2, parts[2]))
    assert isinstance(out, AssembledRecord)
    assert out.after == payload
    assert out.before is None
    assert rs.pending_mains() == 0
    assert rs.pending_chunks() == 0


def test_fragments_arrive_before_main() -> None:
    rs = FragmentReassembler()
    text = json.dumps({"k": "v" * 300})
    parts = _split_text(text, 4)
    # All 4 fragments arrive before the main.
    for i, part in enumerate(parts):
        assert rs.ingest(_fragment("cid", i, part)) is None
    assert rs.pending_chunks() == 1
    main = _chunked_main("tx2", "r2", after=("cid", 4, _crc(text)))
    out = rs.ingest(main)
    assert isinstance(out, AssembledRecord)
    assert out.after == json.loads(text)
    assert rs.pending_chunks() == 0
    assert rs.pending_mains() == 0


def test_before_and_after_both_chunked() -> None:
    rs = FragmentReassembler()
    before = json.dumps({"old": "x" * 100})
    after = json.dumps({"new": "y" * 100})
    main = _chunked_main(
        "tx3",
        "r3",
        before=("b", 2, _crc(before)),
        after=("a", 2, _crc(after)),
    )
    b_parts = _split_text(before, 2)
    a_parts = _split_text(after, 2)
    assert rs.ingest(main) is None
    # Interleave arrivals across the two chunks.
    assert rs.ingest(_fragment("a", 1, a_parts[1])) is None
    assert rs.ingest(_fragment("b", 0, b_parts[0])) is None
    assert rs.ingest(_fragment("a", 0, a_parts[0])) is None
    out = rs.ingest(_fragment("b", 1, b_parts[1]))
    assert isinstance(out, AssembledRecord)
    assert out.before == json.loads(before)
    assert out.after == json.loads(after)


def test_duplicate_fragments_are_deduped() -> None:
    rs = FragmentReassembler()
    text = json.dumps({"v": "x" * 50})
    parts = _split_text(text, 2)
    main = _chunked_main("tx4", "r4", after=("c", 2, _crc(text)))
    assert rs.ingest(main) is None
    assert rs.ingest(_fragment("c", 0, parts[0])) is None
    # Duplicate index 0 arrival should NOT corrupt or emit.
    assert rs.ingest(_fragment("c", 0, parts[0])) is None
    out = rs.ingest(_fragment("c", 1, parts[1]))
    assert isinstance(out, AssembledRecord)
    assert out.after == json.loads(text)


def test_crc_mismatch_raises() -> None:
    rs = FragmentReassembler()
    text = json.dumps({"v": 1})
    parts = _split_text(text, 2)
    main = _chunked_main("tx5", "r5", after=("c", 2, "999999999"))  # bogus crc
    rs.ingest(main)
    rs.ingest(_fragment("c", 0, parts[0]))
    with pytest.raises(CrcMismatchError):
        rs.ingest(_fragment("c", 1, parts[1]))


def test_random_order_1k_fuzz() -> None:
    """1000 chunked records, 3 fragments each, randomized arrival order. Expect zero loss."""
    rng = random.Random(42)
    rs = FragmentReassembler()
    expected: dict[str, dict[str, Any]] = {}
    events: list[FragmentRecord | ChunkedRecord] = []

    for i in range(1000):
        tx_id = f"tx-{i}"
        record_id = f"r-{i}"
        chunk_id = f"c-{i}"
        payload = {"i": i, "pad": "x" * rng.randint(10, 80)}
        text = json.dumps(payload)
        n_parts = rng.choice([2, 3, 4])
        parts = _split_text(text, n_parts)
        main = _chunked_main(tx_id, record_id, after=(chunk_id, n_parts, _crc(text)))
        expected[tx_id] = payload
        events.append(main)
        for idx, part in enumerate(parts):
            events.append(_fragment(chunk_id, idx, part))

    rng.shuffle(events)
    emitted: dict[str, Any] = {}
    for ev in events:
        out = rs.ingest(ev)
        if isinstance(out, AssembledRecord):
            emitted[out.main.source.tx_id] = out.after

    assert emitted == expected
    assert rs.pending_mains() == 0
    assert rs.pending_chunks() == 0


def test_sweep_times_out_incomplete_main() -> None:
    rs = FragmentReassembler(timeout_seconds=1.0)
    text = json.dumps({"k": 1})
    parts = _split_text(text, 3)
    main = _chunked_main("tx-slow", "r-slow", after=("c-slow", 3, _crc(text)))
    rs.ingest(main)
    rs.ingest(_fragment("c-slow", 0, parts[0]))
    # Pretend 2 seconds elapsed by advancing the monotonic clock.
    import time as _t

    future = _t.monotonic() + 2.0
    timeouts = rs.sweep(now_mono=future)
    assert len(timeouts) == 1
    assert timeouts[0].tx_id == "tx-slow"
    assert timeouts[0].reason == "main_timeout"
    assert rs.pending_mains() == 0
    assert rs.pending_chunks() == 0


def test_sweep_times_out_orphan_fragments() -> None:
    rs = FragmentReassembler(timeout_seconds=1.0)
    rs.ingest(_fragment("c-orphan", 0, "{"))
    import time as _t

    future = _t.monotonic() + 2.0
    timeouts = rs.sweep(now_mono=future)
    assert len(timeouts) == 1
    assert timeouts[0].chunk_id == "c-orphan"
    assert timeouts[0].reason == "orphan_timeout"
    assert rs.pending_chunks() == 0
