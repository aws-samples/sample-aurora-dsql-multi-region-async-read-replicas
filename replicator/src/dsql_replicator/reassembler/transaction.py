"""Transaction reassembler.

Each committed source transaction produces N row-records that share a `source.txId` and
each carry the same `transaction.total_records = N`. Records arrive on any shard in any
order (UNORDERED stream). This stage buffers by `tx_id` and emits a `CompletedTransaction`
only when `len(buffered) == N`.

Dedupe: `(tx_id, record_id)` is stable across retries per DSQL CDC guarantees, so a replay
of the same record is silently ignored.

Downstream (ordering buffer / applier) sorts committed transactions globally by
`source.ts_ns`. Within one transaction, records are emitted in ascending `record_id` so
intra-transaction apply order is deterministic (spec I6). The applier may further sort by
`(table, pk)` if it needs human-readable ordering.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from .fragment import AssembledRecord, ReadyRecord


@dataclass(frozen=True)
class CompletedTransaction:
    """A fully-assembled source transaction, ready for ordering/apply."""

    tx_id: str
    ts_ns: int
    total_records: int
    records: tuple[ReadyRecord, ...]

    def __post_init__(self) -> None:
        if len(self.records) != self.total_records:
            raise ValueError(
                f"CompletedTransaction size mismatch: have={len(self.records)} declared={self.total_records}"
            )


@dataclass(frozen=True)
class TransactionTimeout:
    """Returned by `sweep()` for transactions that aged past the configured timeout."""

    tx_id: str
    buffered: int
    expected: int
    reason: str


def _record_key(record: ReadyRecord) -> str:
    """Record-level dedup key within a transaction. Stable across retries per spec."""
    if isinstance(record, AssembledRecord):
        return record.main.transaction.record_id
    return record.transaction.record_id


def _record_ts_ns(record: ReadyRecord) -> int:
    if isinstance(record, AssembledRecord):
        return record.main.source.ts_ns
    return record.source.ts_ns


def _record_tx_id(record: ReadyRecord) -> str:
    if isinstance(record, AssembledRecord):
        return record.main.source.tx_id
    return record.source.tx_id


def _record_total(record: ReadyRecord) -> int:
    if isinstance(record, AssembledRecord):
        return record.main.transaction.total_records
    return record.transaction.total_records


@dataclass
class _TxnBuffer:
    tx_id: str
    expected: int
    ts_ns: int
    records: dict[str, ReadyRecord] = field(default_factory=dict)  # keyed by record_id for dedup
    first_seen_mono: float = field(default_factory=time.monotonic)


class TransactionReassembler:
    """Buffer records by `tx_id`; emit `CompletedTransaction` on last-record arrival.

    `ingest()` returns the completed transaction exactly once per `tx_id`. Duplicate records
    (same `record_id`) and extra records that arrive after completion are ignored.
    """

    def __init__(self, timeout_seconds: float = 60.0) -> None:
        self._timeout_seconds = timeout_seconds
        self._buffers: dict[str, _TxnBuffer] = {}
        # Remember completed tx_ids briefly so late-arriving dupes don't resurrect a buffer.
        # Bounded to the last COMPLETED_LRU entries; sufficient to catch cross-shard replay
        # races on normal timeframes. A larger idempotency table lives downstream.
        self._completed_lru: dict[str, None] = {}

    _COMPLETED_LRU_MAX = 10_000

    # ---- Ingestion -------------------------------------------------------

    def ingest(self, record: ReadyRecord) -> CompletedTransaction | None:
        tx_id = _record_tx_id(record)
        expected = _record_total(record)
        if tx_id in self._completed_lru:
            return None  # late dupe for an already-emitted transaction

        buf = self._buffers.get(tx_id)
        if buf is None:
            buf = _TxnBuffer(tx_id=tx_id, expected=expected, ts_ns=_record_ts_ns(record))
            self._buffers[tx_id] = buf
        elif buf.expected != expected:
            # Should never happen; DSQL guarantees total_records is constant across the txn.
            # Treat as corruption: keep the first value and ignore the mismatch.
            pass

        key = _record_key(record)
        if key not in buf.records:
            buf.records[key] = record

        if len(buf.records) < buf.expected:
            return None

        ordered = tuple(buf.records[k] for k in sorted(buf.records.keys()))
        completed = CompletedTransaction(
            tx_id=tx_id,
            ts_ns=buf.ts_ns,
            total_records=buf.expected,
            records=ordered,
        )
        del self._buffers[tx_id]
        self._mark_completed(tx_id)
        return completed

    def _mark_completed(self, tx_id: str) -> None:
        self._completed_lru[tx_id] = None
        if len(self._completed_lru) > self._COMPLETED_LRU_MAX:
            # Drop oldest; dict preserves insertion order.
            oldest = next(iter(self._completed_lru))
            del self._completed_lru[oldest]

    # ---- Timeouts --------------------------------------------------------

    def sweep(self, now_mono: float | None = None) -> list[TransactionTimeout]:
        now = time.monotonic() if now_mono is None else now_mono
        timed_out: list[TransactionTimeout] = []
        for tx_id, buf in list(self._buffers.items()):
            if now - buf.first_seen_mono >= self._timeout_seconds:
                timed_out.append(
                    TransactionTimeout(
                        tx_id=tx_id,
                        buffered=len(buf.records),
                        expected=buf.expected,
                        reason="txn_timeout",
                    )
                )
                del self._buffers[tx_id]
                # Still mark completed so late arrivals don't re-create the buffer.
                self._mark_completed(tx_id)
        return timed_out

    # ---- Diagnostics -----------------------------------------------------

    def pending_transactions(self) -> int:
        return len(self._buffers)

    def pending_records(self) -> int:
        return sum(len(b.records) for b in self._buffers.values())
