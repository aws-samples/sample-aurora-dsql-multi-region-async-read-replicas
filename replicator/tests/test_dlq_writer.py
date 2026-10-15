"""DLQ writer: payload (de)serialization + SNS fire-and-forget semantics.

Target-DSQL interaction is covered. These
unit tests exercise only logic that doesn't need a live DSQL connection.
"""

from __future__ import annotations

import pytest

from dsql_replicator.dlq.writer import (
    _serialize_record,
    _serialize_txn,
    deserialize_txn,
)
from dsql_replicator.envelope import FullRecord, parse
from dsql_replicator.reassembler import AssembledRecord, CompletedTransaction


def _full(tx_id: str, ts_ns: int, record_id: str = "r0") -> FullRecord:
    rec = parse(
        {
            "type": "full",
            "op": "c",
            "before": None,
            "after": {"id": 1, "name": "hello"},
            "source": {
                "version": "1.1",
                "ts_ms": ts_ns // 1_000_000,
                "ts_ns": ts_ns,
                "txId": tx_id,
                "schema": "public",
                "table": "demo_events",
                "db": "postgres",
                "cluster": "c",
            },
            "ts_ms": ts_ns // 1_000_000,
            "ts_ns": ts_ns,
            "transaction": {"total_records": 1, "record_id": record_id},
        }
    )
    assert isinstance(rec, FullRecord)
    return rec


def _chunked_assembled(tx_id: str, ts_ns: int) -> AssembledRecord:
    from dsql_replicator.envelope import ChunkedRecord

    main = ChunkedRecord.model_validate(
        {
            "type": "chunked",
            "op": "u",
            "before": None,
            "after": None,
            "source": {
                "version": "1.1",
                "ts_ms": ts_ns // 1_000_000,
                "ts_ns": ts_ns,
                "txId": tx_id,
                "schema": "public",
                "table": "big",
                "db": "postgres",
                "cluster": "c",
            },
            "chunked": {
                "after": {
                    "chunk_id": "cid-1",
                    "total_fragments": 1,
                    "crc32c": "00000000",
                }
            },
            "ts_ms": ts_ns // 1_000_000,
            "ts_ns": ts_ns,
            "transaction": {"total_records": 1, "record_id": "r1"},
        }
    )
    return AssembledRecord(main=main, before=None, after={"id": 2, "payload": "big"})


def test_serialize_full_record_round_trip() -> None:
    rec = _full("tx-A", 100)
    txn = CompletedTransaction(tx_id="tx-A", ts_ns=100, total_records=1, records=(rec,))
    blob = _serialize_txn(txn)
    rebuilt = deserialize_txn(blob)
    assert rebuilt.tx_id == "tx-A"
    assert rebuilt.ts_ns == 100
    assert rebuilt.total_records == 1
    assert len(rebuilt.records) == 1
    out = rebuilt.records[0]
    assert isinstance(out, FullRecord)
    assert out.source.tx_id == "tx-A"
    assert out.after == {"id": 1, "name": "hello"}


def test_serialize_assembled_record_round_trip() -> None:
    rec = _chunked_assembled("tx-B", 200)
    txn = CompletedTransaction(tx_id="tx-B", ts_ns=200, total_records=1, records=(rec,))
    blob = _serialize_txn(txn)
    rebuilt = deserialize_txn(blob)
    assert rebuilt.tx_id == "tx-B"
    out = rebuilt.records[0]
    assert isinstance(out, AssembledRecord)
    assert out.after == {"id": 2, "payload": "big"}
    assert out.main.op == "u"


def test_serialize_mixed_records_round_trip() -> None:
    full = _full("tx-C", 300, record_id="r0")
    assembled = _chunked_assembled("tx-C", 300)
    txn = CompletedTransaction(
        tx_id="tx-C",
        ts_ns=300,
        total_records=2,
        records=(full, assembled),
    )
    blob = _serialize_txn(txn)
    rebuilt = deserialize_txn(blob)
    assert isinstance(rebuilt.records[0], FullRecord)
    assert isinstance(rebuilt.records[1], AssembledRecord)


def test_deserialize_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="unknown record kind"):
        deserialize_txn({"tx_id": "x", "ts_ns": 1, "total_records": 1, "records": [{"kind": "mystery"}]})


def test_serialize_record_discriminates_by_type() -> None:
    full = _full("tx-D", 400)
    entry = _serialize_record(full)
    assert entry["kind"] == "full"
    assert entry["payload"]["type"] == "full"

    assembled = _chunked_assembled("tx-D", 400)
    entry2 = _serialize_record(assembled)
    assert entry2["kind"] == "assembled"
    assert entry2["main"]["type"] == "chunked"
    assert entry2["after"] == {"id": 2, "payload": "big"}
