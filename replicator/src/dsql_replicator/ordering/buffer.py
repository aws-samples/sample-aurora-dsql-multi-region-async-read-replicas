"""Ordering buffer.

Reassembled transactions arrive on different shards in different orders. The applier must
commit them in strictly ascending `source.ts_ns` (spec I8). This stage holds a priority
queue keyed by `ts_ns` and releases its head only when both:

  1. `head.ts_ns < W` where `W` is the low watermark across all shards — the smallest
     `ts_ns` any shard may still deliver. We know `head` is safe only when every shard
     has either produced records newer than `head.ts_ns` or declared itself idle under the
     synthetic-advance rule (spec I9).

  2. No open DLQ fence covers `head.ts_ns`. When a fence is set at `fence_ts_ns`, the
     applier stops at `head.ts_ns < fence_ts_ns`; everything from `fence_ts_ns` onward
     waits for operator resolution.

Watermark math per shard:

    effective_ts = latest_ts_ns_seen
    if (monotonic_now - last_advance_mono) >= idle_timeout AND millis_behind_latest == 0:
        synthetic = wall_now_ns - clock_skew_budget_ns
        effective_ts = max(latest_ts_ns_seen, synthetic)

    W = min(effective_ts) across registered shards

Registered-but-never-reported shards contribute `0`, which freezes the pipeline until
the consumer reports at least once per shard. The consumer is expected to call
`update_shard()` every poll loop whether or not records arrived.
"""

from __future__ import annotations

import heapq
import time
from dataclasses import dataclass, field

from dsql_replicator.reassembler import CompletedTransaction


@dataclass
class ShardWatermark:
    """Per-shard watermark state."""

    shard_key: str
    latest_ts_ns: int = 0
    millis_behind_latest: int = 0
    last_advance_mono: float = field(default_factory=time.monotonic)
    last_report_mono: float = field(default_factory=time.monotonic)
    advance_count: int = 0
    idle_synth_count: int = 0


@dataclass(frozen=True)
class OrderingStats:
    """Snapshot for metrics."""

    depth: int
    watermark_ns: int | None
    head_ts_ns: int | None
    fence_ts_ns: int | None
    pending_releases: int
    idle_shards_synthetically_advanced: int


class OrderingBuffer:
    """Priority queue of `CompletedTransaction` with watermark + fence gating."""

    def __init__(
        self,
        *,
        idle_shard_timeout_seconds: float = 30.0,
        clock_skew_budget_seconds: float = 5.0,
        max_depth: int = 100_000,
    ) -> None:
        self._idle_timeout_s = idle_shard_timeout_seconds
        self._clock_skew_ns = int(clock_skew_budget_seconds * 1e9)
        self._max_depth = max_depth

        # Monotonic counter breaks ts_ns ties deterministically (and keeps heapq from
        # trying to order CompletedTransaction objects directly).
        self._seq = 0
        self._heap: list[tuple[int, int, CompletedTransaction]] = []
        # Dedup across re-ingests (should be rare — upstream reassembler already dedupes).
        self._seen_tx_ids: set[str] = set()

        self._shards: dict[str, ShardWatermark] = {}
        self._fence_ts_ns: int | None = None

    # ---- shard tracking --------------------------------------------------

    def register_shard(self, shard_key: str) -> None:
        """Register a shard so its watermark participates in W. Call before polling."""
        self._shards.setdefault(shard_key, ShardWatermark(shard_key=shard_key))

    def forget_shard(self, shard_key: str) -> None:
        """Remove a shard (e.g., after a shard-close in Kinesis). Its watermark stops gating."""
        self._shards.pop(shard_key, None)

    def update_shard(
        self,
        shard_key: str,
        *,
        latest_ts_ns: int,
        millis_behind_latest: int,
        now_mono: float | None = None,
    ) -> None:
        """Record the latest `source.ts_ns` observed on a shard this poll iteration.

        `latest_ts_ns` is the max `ts_ns` across all records this shard has delivered so
        far; it never decreases. Pass the previous value if the current batch was empty.

        `millis_behind_latest` is Kinesis's own metric; a value of 0 unlocks the idle
        synthetic-advance rule.
        """
        now = time.monotonic() if now_mono is None else now_mono
        shard = self._shards.get(shard_key)
        if shard is None:
            shard = ShardWatermark(shard_key=shard_key)
            self._shards[shard_key] = shard
        shard.millis_behind_latest = millis_behind_latest
        shard.last_report_mono = now
        if latest_ts_ns > shard.latest_ts_ns:
            shard.latest_ts_ns = latest_ts_ns
            shard.last_advance_mono = now
            shard.advance_count += 1

    # ---- fence -----------------------------------------------------------

    def set_fence(self, fence_ts_ns: int | None) -> None:
        """Open or close the DLQ fence. `None` means no active fence."""
        self._fence_ts_ns = fence_ts_ns

    @property
    def fence_ts_ns(self) -> int | None:
        return self._fence_ts_ns

    # ---- ingestion -------------------------------------------------------

    def ingest(self, txn: CompletedTransaction) -> None:
        """Add a completed transaction to the ordering heap."""
        if txn.tx_id in self._seen_tx_ids:
            return
        if len(self._heap) >= self._max_depth:
            raise BackpressureError(f"ordering buffer at max depth {self._max_depth}; refusing new ingest")
        self._seq += 1
        heapq.heappush(self._heap, (txn.ts_ns, self._seq, txn))
        self._seen_tx_ids.add(txn.tx_id)

    # ---- release ---------------------------------------------------------

    def ready(
        self, *, now_mono: float | None = None, now_wall_ns: int | None = None
    ) -> list[CompletedTransaction]:
        """Return and pop all transactions whose `ts_ns` is strictly below both W and fence.

        Caller is responsible for applying them in order (they are returned in `ts_ns`
        order already). The applier itself must still COMMIT serially — this method
        batches only the eligibility check, not the apply.
        """
        now_mono = time.monotonic() if now_mono is None else now_mono
        now_wall_ns = time.time_ns() if now_wall_ns is None else now_wall_ns
        w = self._compute_watermark(now_mono, now_wall_ns)
        if w is None:
            return []
        gate = w if self._fence_ts_ns is None else min(w, self._fence_ts_ns)

        out: list[CompletedTransaction] = []
        while self._heap and self._heap[0][0] < gate:
            _ts, _seq, txn = heapq.heappop(self._heap)
            self._seen_tx_ids.discard(txn.tx_id)
            out.append(txn)
        return out

    # ---- diagnostics -----------------------------------------------------

    def _compute_watermark(self, now_mono: float, now_wall_ns: int) -> int | None:
        if not self._shards:
            return None
        effectives: list[int] = []
        for shard in self._shards.values():
            effective = shard.latest_ts_ns
            idle = (now_mono - shard.last_advance_mono) >= self._idle_timeout_s
            if idle and shard.millis_behind_latest == 0:
                synthetic = now_wall_ns - self._clock_skew_ns
                if synthetic > effective:
                    effective = synthetic
                    shard.idle_synth_count += 1
            effectives.append(effective)
        return min(effectives)

    def watermark(self, now_mono: float | None = None, now_wall_ns: int | None = None) -> int | None:
        return self._compute_watermark(
            time.monotonic() if now_mono is None else now_mono,
            time.time_ns() if now_wall_ns is None else now_wall_ns,
        )

    def depth(self) -> int:
        return len(self._heap)

    def head_ts_ns(self) -> int | None:
        return self._heap[0][0] if self._heap else None

    def stats(self, now_mono: float | None = None, now_wall_ns: int | None = None) -> OrderingStats:
        w = self.watermark(now_mono, now_wall_ns)
        return OrderingStats(
            depth=self.depth(),
            watermark_ns=w,
            head_ts_ns=self.head_ts_ns(),
            fence_ts_ns=self._fence_ts_ns,
            pending_releases=sum(
                1
                for ts, _, _ in self._heap
                if w is not None and ts < (w if self._fence_ts_ns is None else min(w, self._fence_ts_ns))
            ),
            idle_shards_synthetically_advanced=sum(s.idle_synth_count for s in self._shards.values()),
        )


class BackpressureError(Exception):
    """Raised when the ordering buffer is at max depth and refuses new ingests."""
