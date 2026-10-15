"""CDC envelope types mirroring the Aurora DSQL CDC JSON format (UG pp.407-414).

A Kinesis `Data` payload is a JSON object discriminated by a top-level `type` field:
`full`, `chunked`, or `fragment`. We model this with a Pydantic discriminated union so
unknown shapes raise a structured validation error that routes to the DLQ.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from .errors import EnvelopeError

type OpCode = Literal["c", "u", "d"]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)


class Source(_Strict):
    version: str
    ts_ms: int
    ts_ns: int
    tx_id: str = Field(alias="txId")
    schema_name: str = Field(alias="schema")
    table: str
    db: str
    cluster: str


class Transaction(_Strict):
    total_records: int
    record_id: str


class ChunkedImage(_Strict):
    chunk_id: str
    total_fragments: int
    crc32c: str


class ChunkedImages(_Strict):
    before: ChunkedImage | None = None
    after: ChunkedImage | None = None


class FullRecord(_Strict):
    type: Literal["full"]
    op: OpCode
    before: Any | None = None
    after: Any | None = None
    source: Source
    ts_ms: int
    ts_ns: int
    transaction: Transaction


class ChunkedRecord(_Strict):
    type: Literal["chunked"]
    op: OpCode
    before: Any | None = None
    after: Any | None = None
    source: Source
    chunked: ChunkedImages
    ts_ms: int
    ts_ns: int
    transaction: Transaction


class FragmentRecord(_Strict):
    type: Literal["fragment"]
    chunk_id: str
    index: int
    data: str


type CdcRecord = Annotated[
    FullRecord | ChunkedRecord | FragmentRecord,
    Field(discriminator="type"),
]

_adapter: TypeAdapter[CdcRecord] = TypeAdapter(CdcRecord)


def parse(raw: bytes | str | dict[str, Any]) -> CdcRecord:
    """Parse a Kinesis record's Data payload into a typed CDC envelope."""
    try:
        if isinstance(raw, bytes | bytearray):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            raw = json.loads(raw)
        return _adapter.validate_python(raw)
    except (ValidationError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise EnvelopeError(f"not a valid CDC envelope: {exc}") from exc


def tx_id(record: CdcRecord) -> str | None:
    """Return the transaction id for records that carry one. Fragments do not."""
    if isinstance(record, FragmentRecord):
        return None
    return record.source.tx_id


def ts_ns(record: CdcRecord) -> int | None:
    if isinstance(record, FragmentRecord):
        return None
    return record.source.ts_ns
