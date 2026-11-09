"""Writer-pool autoscaler.

Watches end-to-end replication lag and resizes the Applier's connection pool
in-process so hot sources get more apply concurrency without needing a task
restart or operator intervention. Scale decisions are sticky — we require the
threshold to be exceeded for the full sustain window before stepping, so a
transient burst does not thrash the pool.

Rules:
  - Scale UP by `step` when lag_ms > scale_up_lag_ms for the whole
    scale_up_sustain window, up to the applier's max_worker_count.
  - Scale DOWN by `step` when lag_ms < scale_down_lag_ms for the whole
    scale_down_sustain window, down to the applier's starting floor.

The lag signal is injected as a callable so this stays decoupled from the
pipeline / ordering buffer wiring (and so it is cheap to unit test).
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

import structlog

from .apply import Applier

log = structlog.get_logger("applier.autoscaler")


@dataclass(frozen=True)
class AutoscaleParams:
    floor: int
    ceiling: int
    step: int
    tick_s: float
    scale_up_lag_ms: float
    scale_up_sustain_s: float
    scale_down_lag_ms: float
    scale_down_sustain_s: float


class WorkerAutoscaler:
    """Grow/shrink the Applier connection pool based on sustained lag."""

    def __init__(
        self,
        applier: Applier,
        lag_ms_signal: Callable[[], float | None],
        params: AutoscaleParams,
        stop_event: asyncio.Event,
    ) -> None:
        self._applier = applier
        self._lag_ms = lag_ms_signal
        self._p = params
        self._stop = stop_event

        # One sample per tick; keep enough history to span the longer window.
        self._window_s = max(params.scale_up_sustain_s, params.scale_down_sustain_s)
        maxlen = max(1, int(self._window_s / params.tick_s) + 2)
        self._samples: deque[tuple[float, float]] = deque(maxlen=maxlen)

    async def run(self) -> None:
        if self._p.ceiling <= self._p.floor:
            log.info(
                "autoscaler.disabled",
                reason="ceiling<=floor",
                floor=self._p.floor,
                ceiling=self._p.ceiling,
            )
            return
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._p.tick_s)
                return
            except TimeoutError:
                pass

            lag = self._lag_ms()
            if lag is None:
                # No signal yet (watermark not advanced) — nothing to decide.
                continue
            now = time.monotonic()
            self._samples.append((now, lag))

            if self._should_scale_up(now):
                added = await self._applier.grow_pool(self._p.step)
                if added:
                    log.info(
                        "autoscaler.scaled_up",
                        delta=added,
                        workers=self._applier.worker_count,
                        lag_ms=lag,
                    )
                    self._samples.clear()  # restart the sustain window
            elif self._should_scale_down(now):
                removed = await self._applier.shrink_pool(self._p.step)
                if removed:
                    log.info(
                        "autoscaler.scaled_down",
                        delta=removed,
                        workers=self._applier.worker_count,
                        lag_ms=lag,
                    )
                    self._samples.clear()

    def _should_scale_up(self, now: float) -> bool:
        if self._applier.worker_count >= self._p.ceiling:
            return False
        return self._all_in_window_satisfy(
            now,
            self._p.scale_up_sustain_s,
            lambda v: v > self._p.scale_up_lag_ms,
        )

    def _should_scale_down(self, now: float) -> bool:
        if self._applier.worker_count <= self._p.floor:
            return False
        return self._all_in_window_satisfy(
            now,
            self._p.scale_down_sustain_s,
            lambda v: v < self._p.scale_down_lag_ms,
        )

    def _all_in_window_satisfy(
        self, now: float, window_s: float, predicate: Callable[[float], bool]
    ) -> bool:
        cutoff = now - window_s
        in_window = [v for t, v in self._samples if t >= cutoff]
        # Require the window to actually be filled before deciding, so we
        # don't scale on a single sample right after startup.
        min_samples = max(1, int(window_s / self._p.tick_s))
        if len(in_window) < min_samples:
            return False
        return all(predicate(v) for v in in_window)
