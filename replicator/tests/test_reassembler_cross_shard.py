"""Regression test for the per-shard FragmentReassembler bug (review issue #4).

DSQL CDC distributes a single chunked record's main + fragments across any open
shard in an UNORDERED stream. The pipeline used to instantiate one
`FragmentReassembler` per shard worker, which silently dropped every chunked
record whose fragments landed on a different shard than the main — a latent
data-loss bug whose non-triggering depended on undocumented publisher behavior.

These tests pin the fix in two places:

1.  The reassembler is shard-agnostic by design — feeding fragments from any
    source to ONE shared instance completes the chunk. (Positive proof.)
2.  Feeding the same fragments to SEPARATE per-shard instances (the old
    behavior) never completes — proving the bug was real. (Negative proof.)
3.  `ConsumerPipeline.__init__` wires a single process-global
    `self._fragment`, not a per-shard one. (Structural proof.)
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

from crc32c import crc32c as _crc32c

from dsql_replicator.consumer.pipeline import ConsumerPipeline
from dsql_replicator.envelope import ChunkedRecord, FragmentRecord, parse
from dsql_replicator.reassembler import AssembledRecord, FragmentReassembler


def _crc(text: str) -> str:
    return str(_crc32c(text.encode("utf-8")))


def _split(text: str, parts: int) -> list[str]:
    step = max(1, len(text) // parts)
    chunks = [text[i : i + step] for i in range(0, len(text), step)]
    while len(chunks) < parts:
        chunks.append("")
    if len(chunks) > parts:
        chunks[parts - 1] = "".join(chunks[parts - 1 :])
        chunks = chunks[:parts]
    assert "".join(chunks) == text
    return chunks


def _chunked_main(tx_id: str, record_id: str, chunk_id: str, total: int, crc: str) -> ChunkedRecord:
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
            "chunked": {"after": {"chunk_id": chunk_id, "total_fragments": total, "crc32c": crc}},
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


# --- 1. positive proof -----------------------------------------------------


def test_single_shared_reassembler_completes_fragments_from_any_source() -> None:
    """A single FragmentReassembler assembles a chunked record regardless of which
    simulated shard delivers each piece."""
    text = json.dumps({"order_id": 42, "payload": "x" * 400})
    parts = _split(text, 3)
    shared = FragmentReassembler()
    main = _chunked_main("tx1", "r1", chunk_id="c-after", total=3, crc=_crc(text))

    # Interleave arrivals as if from three different shards.
    assert shared.ingest(_fragment("c-after", 0, parts[0])) is None  # "shard A"
    assert shared.ingest(main) is None                                # "shard B"
    assert shared.ingest(_fragment("c-after", 1, parts[1])) is None  # "shard C"
    out = shared.ingest(_fragment("c-after", 2, parts[2]))            # "shard A"
    assert isinstance(out, AssembledRecord)
    assert out.after == {"order_id": 42, "payload": "x" * 400}


# --- 2. negative proof: the OLD per-shard model loses data -----------------


def test_per_shard_reassemblers_lose_cross_shard_fragments() -> None:
    """Simulates the pre-fix wiring: one FragmentReassembler per shard. A chunked
    record whose fragments cross shards never completes anywhere."""
    text = json.dumps({"order_id": 7, "payload": "y" * 300})
    parts = _split(text, 3)
    crc = _crc(text)
    per_shard = {
        "shard-A": FragmentReassembler(),
        "shard-B": FragmentReassembler(),
        "shard-C": FragmentReassembler(),
    }

    main = _chunked_main("tx2", "r1", chunk_id="c-after", total=3, crc=crc)
    # Each shard sees only its own piece.
    assert per_shard["shard-A"].ingest(main) is None
    assert per_shard["shard-B"].ingest(_fragment("c-after", 0, parts[0])) is None
    assert per_shard["shard-B"].ingest(_fragment("c-after", 1, parts[1])) is None
    assert per_shard["shard-C"].ingest(_fragment("c-after", 2, parts[2])) is None

    # None of the per-shard reassemblers can emit — the main sees no fragments,
    # the fragment-only shards never see the main. This is the lost-txn case.
    for rs in per_shard.values():
        assert rs.pending_mains() + rs.pending_chunks() > 0


# --- 3. structural proof: pipeline wires one global FragmentReassembler ----


def test_pipeline_wires_single_process_global_fragment_reassembler() -> None:
    """`ConsumerPipeline.__init__` must allocate the FragmentReassembler; shard
    workers must NOT. Static source inspection — the init path pulls in
    aioboto3 and MetricsEmitter, which is more weight than a structural guard
    needs. A one-line regression in `_shard_worker` is what this guards."""
    import inspect

    init_src = inspect.getsource(ConsumerPipeline.__init__)
    worker_src = inspect.getsource(ConsumerPipeline._shard_worker)
    assert "self._fragment = FragmentReassembler(" in init_src, (
        "ConsumerPipeline.__init__ must allocate a process-global "
        "`self._fragment = FragmentReassembler(...)` next to `self._txn`."
    )
    assert "FragmentReassembler(" not in worker_src, (
        "ConsumerPipeline._shard_worker must not allocate a per-shard "
        "FragmentReassembler; use the process-global self._fragment."
    )


# Silence unused imports when running under linters.
_ = (Any, MagicMock, ConsumerPipeline)
