"""Reassembly stage: oversized-record fragments.

Row-level LWW replaced the former transaction-reassembly stage in the
2026-11 simplification pass (Kinesis cannot preserve DSQL transaction
order across shards). What remains is per-record fragment reassembly for
oversized rows (>1 MB) that Aurora DSQL splits into a chunked main plus
one or more fragment records.
"""

from __future__ import annotations

from .fragment import (
    AssembledRecord,
    CrcMismatchError,
    FragmentReassembler,
    FragmentTimeout,
    ReadyRecord,
    chunk_ids_of,
)

__all__ = [
    "AssembledRecord",
    "CrcMismatchError",
    "FragmentReassembler",
    "FragmentTimeout",
    "ReadyRecord",
    "chunk_ids_of",
]
