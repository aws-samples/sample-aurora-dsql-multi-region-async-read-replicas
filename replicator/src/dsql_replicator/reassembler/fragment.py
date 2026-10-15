"""Fragment reassembler.

Oversized CDC records are split by Aurora DSQL into a `chunked` main record plus one or
more `fragment` records that share a `chunk_id`. The main record's top-level `before` or
`after` is `null`; the actual chunked descriptor lives under `chunked.before` /
`chunked.after`, giving us `chunk_id`, `total_fragments`, and the CRC32C checksum of the
reassembled image text.

Fragments and main can arrive on different shards in any order (UNORDERED stream). This
module buffers them, verifies CRC32C on complete chunks, and emits an `AssembledRecord`
whose `before` / `after` are the parsed JSON values.

This stage is single-threaded by design — callers are expected to feed it from one
consumer task per shard-group. For multi-shard fan-in, run one assembler per logical
partition or guard with an external lock.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from crc32c import crc32c as _crc32c

from dsql_replicator.envelope import ChunkedRecord, FragmentRecord, FullRecord
from dsql_replicator.errors import ReplicatorError


class CrcMismatchError(ReplicatorError):
    """A reassembled chunk's CRC32C did not match the main record's declared checksum."""


@dataclass(frozen=True)
class AssembledRecord:
    """A ChunkedRecord whose before/after chunked images have been reassembled."""

    main: ChunkedRecord
    before: Any
    after: Any

    @property
    def tx_id(self) -> str:
        return self.main.source.tx_id

    @property
    def ts_ns(self) -> int:
        return self.main.source.ts_ns


ReadyRecord = FullRecord | AssembledRecord


@dataclass(frozen=True)
class FragmentTimeout:
    """Returned by `sweep()` for buffers that aged past the configured timeout."""

    chunk_id: str
    tx_id: str | None
    record_id: str | None
    reason: str


@dataclass
class _ChunkBuffer:
    """Accumulates fragment data for one chunk_id."""

    fragments: dict[int, str] = field(default_factory=dict)
    total: int | None = None
    crc32c_expected: str | None = None
    first_seen_mono: float = field(default_factory=time.monotonic)

    def complete(self) -> bool:
        return self.total is not None and len(self.fragments) == self.total

    def reassemble(self) -> Any:
        """Concatenate fragments in index order, verify CRC32C, parse as JSON."""
        if not self.complete():
            raise RuntimeError("reassemble() called before chunk complete")
        assert self.total is not None  # for mypy
        parts: list[str] = []
        for i in range(self.total):
            parts.append(self.fragments[i])
        text = "".join(parts)
        if self.crc32c_expected is not None:
            computed = _crc32c(text.encode("utf-8"))
            if str(computed) != self.crc32c_expected:
                raise CrcMismatchError(
                    f"CRC32C mismatch: expected={self.crc32c_expected} computed={computed}"
                )
        return json.loads(text)


_MainKey = tuple[str, str]  # (tx_id, record_id)


@dataclass
class _MainBuffer:
    """A pending `type=chunked` main record whose chunked images we are still assembling."""

    main: ChunkedRecord
    before_chunk_id: str | None
    after_chunk_id: str | None
    first_seen_mono: float = field(default_factory=time.monotonic)


class FragmentReassembler:
    """Buffer fragments + chunked main records; emit `AssembledRecord` when complete.

    `FullRecord`s pass straight through.
    """

    def __init__(self, timeout_seconds: float = 120.0) -> None:
        self._timeout_seconds = timeout_seconds
        # chunk_id -> buffer for its fragments. Entries live until the main record emits.
        self._chunks: dict[str, _ChunkBuffer] = {}
        # Reverse index: chunk_id -> (tx_id, record_id) of the main that owns it. Populated
        # when the main arrives. Fragments arriving before the main are "orphans" — they
        # live in self._chunks without this mapping until the main shows up.
        self._chunk_to_main: dict[str, _MainKey] = {}
        # Pending main records keyed by (tx_id, record_id).
        self._mains: dict[_MainKey, _MainBuffer] = {}

    # ---- Ingestion -------------------------------------------------------

    def ingest(self, record: FullRecord | ChunkedRecord | FragmentRecord) -> ReadyRecord | None:
        """Feed one CDC record. Returns a ready record if ingestion completed one.

        - FullRecord → returned verbatim (nothing to assemble).
        - ChunkedRecord → buffered; returns AssembledRecord if every referenced chunk is
          already complete at ingest time (unusual but possible for 0-fragment chunks).
        - FragmentRecord → buffered; returns AssembledRecord of the parent main if this
          fragment was the last piece needed.
        """
        if isinstance(record, FullRecord):
            return record
        if isinstance(record, ChunkedRecord):
            return self._on_main(record)
        return self._on_fragment(record)

    def _on_main(self, main: ChunkedRecord) -> AssembledRecord | None:
        key: _MainKey = (main.source.tx_id, main.transaction.record_id)
        before_id = main.chunked.before.chunk_id if main.chunked.before else None
        after_id = main.chunked.after.chunk_id if main.chunked.after else None
        self._mains[key] = _MainBuffer(main=main, before_chunk_id=before_id, after_chunk_id=after_id)

        for chunk_id, descriptor in [
            (before_id, main.chunked.before),
            (after_id, main.chunked.after),
        ]:
            if chunk_id is None or descriptor is None:
                continue
            self._chunk_to_main[chunk_id] = key
            buf = self._chunks.setdefault(chunk_id, _ChunkBuffer())
            buf.total = descriptor.total_fragments
            buf.crc32c_expected = descriptor.crc32c

        return self._try_emit(key)

    def _on_fragment(self, frag: FragmentRecord) -> AssembledRecord | None:
        buf = self._chunks.setdefault(frag.chunk_id, _ChunkBuffer())
        # Dedupe: same (chunk_id, index) → silently ignore replays.
        if frag.index not in buf.fragments:
            buf.fragments[frag.index] = frag.data
        key = self._chunk_to_main.get(frag.chunk_id)
        if key is None:
            return None  # orphan; wait for main
        return self._try_emit(key)

    def _try_emit(self, key: _MainKey) -> AssembledRecord | None:
        mb = self._mains.get(key)
        if mb is None:
            return None
        chunk_ids = [cid for cid in (mb.before_chunk_id, mb.after_chunk_id) if cid is not None]
        for cid in chunk_ids:
            buf = self._chunks.get(cid)
            if buf is None or not buf.complete():
                return None

        # All chunks complete — reassemble (may raise CrcMismatchError).
        before_val = self._chunks[mb.before_chunk_id].reassemble() if mb.before_chunk_id else mb.main.before
        after_val = self._chunks[mb.after_chunk_id].reassemble() if mb.after_chunk_id else mb.main.after
        # Clear state for this main.
        for cid in chunk_ids:
            self._chunks.pop(cid, None)
            self._chunk_to_main.pop(cid, None)
        del self._mains[key]

        return AssembledRecord(main=mb.main, before=before_val, after=after_val)

    # ---- Timeouts --------------------------------------------------------

    def sweep(self, now_mono: float | None = None) -> list[FragmentTimeout]:
        """Return and purge buffers older than `timeout_seconds`.

        Caller is responsible for routing the associated records to the DLQ.
        """
        now = time.monotonic() if now_mono is None else now_mono
        timed_out: list[FragmentTimeout] = []

        # Expire whole pending mains (and their chunks).
        for key, mb in list(self._mains.items()):
            if now - mb.first_seen_mono >= self._timeout_seconds:
                for cid in (mb.before_chunk_id, mb.after_chunk_id):
                    if cid is not None:
                        self._chunks.pop(cid, None)
                        self._chunk_to_main.pop(cid, None)
                del self._mains[key]
                timed_out.append(
                    FragmentTimeout(
                        chunk_id=mb.before_chunk_id or mb.after_chunk_id or "",
                        tx_id=key[0],
                        record_id=key[1],
                        reason="main_timeout",
                    )
                )

        # Expire orphan chunks (no main ever arrived).
        for cid, buf in list(self._chunks.items()):
            if cid in self._chunk_to_main:
                continue  # tied to a main, handled above
            if now - buf.first_seen_mono >= self._timeout_seconds:
                self._chunks.pop(cid, None)
                timed_out.append(
                    FragmentTimeout(chunk_id=cid, tx_id=None, record_id=None, reason="orphan_timeout")
                )

        return timed_out

    # ---- Diagnostics -----------------------------------------------------

    def pending_mains(self) -> int:
        return len(self._mains)

    def pending_chunks(self) -> int:
        return len(self._chunks)


def chunk_ids_of(main: ChunkedRecord) -> list[str]:
    """Convenience: all chunk_ids referenced by a chunked main record."""
    out: list[str] = []
    if main.chunked.before is not None:
        out.append(main.chunked.before.chunk_id)
    if main.chunked.after is not None:
        out.append(main.chunked.after.chunk_id)
    return out
