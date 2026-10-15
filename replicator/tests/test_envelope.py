from __future__ import annotations

import json

import pytest

from dsql_replicator.envelope import (
    FragmentRecord,
    FullRecord,
    parse,
    ts_ns,
    tx_id,
)
from dsql_replicator.errors import EnvelopeError

_INSERT = {
    "type": "full",
    "op": "c",
    "before": None,
    "after": {"order_id": 1001, "item_id": 42, "quantity": 5, "price": "29.99"},
    "source": {
        "version": "1.1",
        "ts_ms": 1705318200000,
        "ts_ns": 1705318200000000000,
        "txId": "ffthunp5stx6ffs2vyfqoatmfu",
        "schema": "public",
        "table": "order_items",
        "db": "postgres",
        "cluster": "kmabugltfmjdaj2siqr2qbxgju",
    },
    "ts_ms": 1705318200125,
    "ts_ns": 1705318200125483291,
    "transaction": {"total_records": 1, "record_id": "8f14e45fceea167a5a36dedd4bea2543"},
}

_DELETE = {
    "type": "full",
    "op": "d",
    "before": {"order_id": 1001, "item_id": 42},
    "after": None,
    "source": {
        "version": "1.1",
        "ts_ms": 1,
        "ts_ns": 1,
        "txId": "x",
        "schema": "public",
        "table": "t",
        "db": "postgres",
        "cluster": "c",
    },
    "ts_ms": 2,
    "ts_ns": 2,
    "transaction": {"total_records": 1, "record_id": "r"},
}

_FRAGMENT = {"type": "fragment", "chunk_id": "cid", "index": 2, "data": '{"x":1'}


def test_parses_insert_fixture() -> None:
    rec = parse(_INSERT)
    assert isinstance(rec, FullRecord)
    assert rec.op == "c"
    assert rec.before is None
    assert rec.source.tx_id == "ffthunp5stx6ffs2vyfqoatmfu"
    assert rec.transaction.total_records == 1


def test_parses_delete_pk_only_before() -> None:
    rec = parse(_DELETE)
    assert isinstance(rec, FullRecord)
    assert rec.op == "d"
    assert rec.before == {"order_id": 1001, "item_id": 42}
    assert rec.after is None


def test_parses_fragment() -> None:
    rec = parse(_FRAGMENT)
    assert isinstance(rec, FragmentRecord)
    assert rec.chunk_id == "cid"
    assert rec.index == 2
    assert rec.data == '{"x":1'


def test_parses_from_bytes_and_str() -> None:
    raw = json.dumps(_INSERT).encode()
    rec = parse(raw)
    assert isinstance(rec, FullRecord)
    rec2 = parse(json.dumps(_INSERT))
    assert isinstance(rec2, FullRecord)


def test_rejects_unknown_type() -> None:
    with pytest.raises(EnvelopeError):
        parse({"type": "bogus"})


def test_rejects_malformed_json() -> None:
    with pytest.raises(EnvelopeError):
        parse(b"{not json")


def test_tx_id_and_ts_ns_helpers() -> None:
    rec = parse(_INSERT)
    assert tx_id(rec) == "ffthunp5stx6ffs2vyfqoatmfu"
    assert ts_ns(rec) == 1705318200000000000
    frag = parse(_FRAGMENT)
    assert tx_id(frag) is None
    assert ts_ns(frag) is None
