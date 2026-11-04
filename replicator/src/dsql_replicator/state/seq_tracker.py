"""Per-shard in-flight Kinesis sequence tracking (review issue #1).

**Problem.** Prior to this module, the pipeline flushed the checkpoint on a timer
to the latest *consumed* Kinesis sequence — even for records still buffered in
the fragment reassembler, the transaction reassembler, or the ordering buffer.
A crash with the checkpoint ahead of those buffers permanently lost the
buffered records on restart.

**Fix.** We now track every record from parse through final retirement
(applied-and-committed, or ingest-DLQ'd). The pipeline uses this tracker to
compute a per-shard **safe checkpoint**: the largest consumed Kinesis sequence
on a shard such that every record with a smaller sequence has been fully
retired. The checkpoint upsert runs inside the applier's commit transaction via
`pre_commit_hook`, so a crash immediately after commit means the restarted
process will re-fetch at most one Kinesis batch, and the LWW stamp dedupes it.

**Enroll keys.** Fragments arrive without a `tx_id` / `record_id`, so we accept
multiple keys per enroll:

- `(tx_id, record_id)` — Full records and chunked main records.
- `chunk_id` — fragment records; the chunk is what identifies them.
- `tx_id` — fallback retire key for whole-transaction timeouts.

Retire paths:

- `retire_committed(records)` — the applier is about to COMMIT a batch.
- `retire_ingest_reject(...)` — parse error, CRC mismatch, fragment timeout,
  or txn-reassembly timeout. The record never reached the applier.

Thread-safety: single-process consumer; the pipeline shard worker and the
apply runner are both coroutines on the same event loop, so plain dicts are
fine. No lock is needed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class _InFlight:
    """One parsed record awaiting final retirement."""

    shard_id: str
    seq_str: str
    seq_int: int
    tx_id: str | None
    record_id: str | None
    chunk_ids: tuple[str, ...] = ()


@dataclass
class _ShardState:
    """Per-shard in-flight accounting.

    - `in_flight`: `seq_int -> _InFlight` for records consumed but not retired.
    - `retired_seq_strs`: `seq_int -> seq_str` for records that HAVE retired.
      Kept so we can translate a safe-high-water seq_int back to the exact
      string Kinesis issued (AFTER_SEQUENCE_NUMBER demands a real seq).
    - `max_consumed_seq_str` / `_int`: the newest Kinesis seq we've seen on
      this shard. Used when nothing is in-flight.
    - `last_safe_checkpoint`: the latest seq we've already committed as safe.
      Monotonic — we never regress a checkpoint, so we only emit a new value
      when it strictly advances.
    """

    in_flight: dict[int, _InFlight] = field(default_factory=dict)
    retired_seq_strs: dict[int, str] = field(default_factory=dict)
    max_consumed_seq_str: str | None = None
    max_consumed_seq_int: int = -1
    last_safe_checkpoint_int: int = -1
    last_safe_checkpoint_str: str | None = None


class ShardSeqTracker:
    """Per-shard in-flight Kinesis sequence tracker.

    Not thread-safe by design — the single consumer event loop is the only
    writer. Call `enroll` on every parsed record, and either `retire_committed`
    (apply success) or `retire_ingest_reject` (parse/CRC/timeout) on every
    terminal event. `safe_checkpoint(shard_id)` returns the max seq that is
    strictly safe to persist, or None if the shard has nothing new to report.
    """

    def __init__(self) -> None:
        self._shards: dict[str, _ShardState] = {}
        # Reverse indexes so retire operations are O(1) in the usual case:
        self._by_tx_record: dict[tuple[str, str], list[int]] = {}
        self._by_chunk: dict[str, list[int]] = {}
        self._by_tx: dict[str, list[int]] = {}
        # Reverse map keyed-seq -> shard so retire doesn't need the shard id.
        self._seq_shard: dict[int, str] = {}

    # ---- enrollment -----------------------------------------------------

    def enroll(
        self,
        *,
        shard_id: str,
        seq_str: str,
        tx_id: str | None = None,
        record_id: str | None = None,
        chunk_ids: tuple[str, ...] = (),
    ) -> None:
        """Record that a Kinesis sequence has entered the pipeline. Idempotent
        on the exact `(shard_id, seq_str)` pair — a replay from a reset
        iterator is treated as the same enrollment."""
        try:
            seq_int = int(seq_str)
        except (TypeError, ValueError):
            # Non-numeric sequence — Kinesis seqs are decimal strings in
            # practice, but a test fake or future format change shouldn't crash
            # the pipeline. We fall back to a hash-order key just so retire
            # still works; the safe-checkpoint computation becomes
            # best-effort for that record.
            seq_int = hash(seq_str)
        shard = self._shards.setdefault(shard_id, _ShardState())
        if seq_int in shard.in_flight:
            # Already enrolled.
            return
        shard.in_flight[seq_int] = _InFlight(
            shard_id=shard_id,
            seq_str=seq_str,
            seq_int=seq_int,
            tx_id=tx_id,
            record_id=record_id,
            chunk_ids=tuple(cid for cid in chunk_ids if cid),
        )
        self._seq_shard[seq_int] = shard_id
        if seq_int > shard.max_consumed_seq_int:
            shard.max_consumed_seq_int = seq_int
            shard.max_consumed_seq_str = seq_str
        if tx_id and record_id:
            self._by_tx_record.setdefault((tx_id, record_id), []).append(seq_int)
        if tx_id:
            self._by_tx.setdefault(tx_id, []).append(seq_int)
        for cid in chunk_ids:
            if cid:
                self._by_chunk.setdefault(cid, []).append(seq_int)

    # ---- retirement -----------------------------------------------------

    def retire_by_tx_record(self, tx_id: str, record_id: str) -> set[str]:
        """Retire every seq enrolled under `(tx_id, record_id)`. Returns the set
        of shards whose in-flight set changed, so callers can checkpoint them."""
        return self._retire_seqs(self._by_tx_record.pop((tx_id, record_id), ()))

    def retire_by_tx(self, tx_id: str) -> set[str]:
        return self._retire_seqs(self._by_tx.pop(tx_id, ()))

    def retire_by_chunk(self, chunk_id: str) -> set[str]:
        return self._retire_seqs(self._by_chunk.pop(chunk_id, ()))

    def retire_by_seq(self, shard_id: str, seq_str: str) -> set[str]:
        """Retire a single `(shard_id, seq_str)` pair directly. Used for parse
        errors where we have no tx_id / chunk_id to look up by."""
        if shard_id not in self._shards:
            return set()
        try:
            seq_int = int(seq_str)
        except (TypeError, ValueError):
            seq_int = hash(seq_str)
        return self._retire_seqs([seq_int])

    def _retire_seqs(self, seq_ints: Any) -> set[str]:
        touched: set[str] = set()
        for seq_int in seq_ints:
            shard_id = self._seq_shard.pop(seq_int, None)
            if shard_id is None:
                continue
            shard = self._shards.get(shard_id)
            if shard is None:
                continue
            info = shard.in_flight.pop(seq_int, None)
            if info is None:
                continue
            shard.retired_seq_strs[seq_int] = info.seq_str
            touched.add(shard_id)
        return touched

    # ---- safe-checkpoint computation ------------------------------------

    def safe_checkpoint(self, shard_id: str) -> tuple[str, int] | None:
        """Return `(seq_str, seq_int)` safe to persist for this shard, or None
        if there is nothing new to persist (never consumed, or no advance).

        **Safe seq definition.** The returned seq is the largest retired
        sequence on this shard such that every in-flight sequence is strictly
        greater. If nothing is in-flight, we can advance to the max consumed
        sequence; this is the common steady-state case.
        """
        shard = self._shards.get(shard_id)
        if shard is None:
            return None
        if not shard.in_flight:
            # Everything we've ever consumed on this shard has retired.
            if shard.max_consumed_seq_int <= shard.last_safe_checkpoint_int:
                return None
            if shard.max_consumed_seq_str is None:
                return None
            shard.last_safe_checkpoint_int = shard.max_consumed_seq_int
            shard.last_safe_checkpoint_str = shard.max_consumed_seq_str
            return shard.max_consumed_seq_str, shard.max_consumed_seq_int
        min_in_flight = min(shard.in_flight.keys())
        # Pick the max retired seq strictly less than min_in_flight.
        best_int: int = -1
        best_str: str | None = None
        for s_int, s_str in shard.retired_seq_strs.items():
            if s_int < min_in_flight and s_int > best_int:
                best_int = s_int
                best_str = s_str
        if best_int <= shard.last_safe_checkpoint_int or best_str is None:
            return None
        shard.last_safe_checkpoint_int = best_int
        shard.last_safe_checkpoint_str = best_str
        return best_str, best_int

    # ---- diagnostics ----------------------------------------------------

    def in_flight_count(self, shard_id: str) -> int:
        shard = self._shards.get(shard_id)
        return 0 if shard is None else len(shard.in_flight)

    def forget_shard(self, shard_id: str) -> None:
        """Drop all state for a shard — only call this when the shard worker
        has released its lease and no more records can arrive from it."""
        shard = self._shards.pop(shard_id, None)
        if shard is None:
            return
        for seq_int in list(shard.in_flight.keys()):
            self._seq_shard.pop(seq_int, None)
        for seq_int in list(shard.retired_seq_strs.keys()):
            self._seq_shard.pop(seq_int, None)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Lightweight debug snapshot. Not stable public API."""
        out: dict[str, dict[str, Any]] = {}
        for shard_id, shard in self._shards.items():
            out[shard_id] = {
                "in_flight": len(shard.in_flight),
                "retired": len(shard.retired_seq_strs),
                "max_consumed": shard.max_consumed_seq_str,
                "last_safe_checkpoint": shard.last_safe_checkpoint_str,
            }
        return out
