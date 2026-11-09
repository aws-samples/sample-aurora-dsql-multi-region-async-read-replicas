"""Reassembly stage: oversized-record fragments.

Row-level LWW replaced the former transaction-reassembly stage in the
2026-11 simplification pass (Kinesis cannot preserve DSQL transaction
order across shards). What remains is per-record fragment reassembly for
oversized rows (>1 MB) that Aurora DSQL splits into a chunked main plus
one or more fragment records.

`CompletedTransaction` is retained as a thin DLQ-payload shim so the DLQ
module keeps loading until it is removed in the follow-up commit; it has
no reassembler driving it and the main pipeline no longer produces one.
"""

from __future__ import annotations

from dataclasses import dataclass

from .fragment import (
    AssembledRecord,
    CrcMismatchError,
    FragmentReassembler,
    FragmentTimeout,
    ReadyRecord,
    chunk_ids_of,
)


@dataclass(frozen=True)
class CompletedTransaction:
    """DLQ payload shim. Carries a tuple of records previously stored under one
    DLQ-fence tx_id. The main pipeline no longer produces these — this type
    only survives so the (deprecated, removed in the next commit) DLQ
    serialization helpers keep loading."""

    tx_id: str
    ts_ns: int
    total_records: int
    records: tuple[ReadyRecord, ...]


__all__ = [
    "AssembledRecord",
    "CompletedTransaction",
    "CrcMismatchError",
    "FragmentReassembler",
    "FragmentTimeout",
    "ReadyRecord",
    "chunk_ids_of",
]
