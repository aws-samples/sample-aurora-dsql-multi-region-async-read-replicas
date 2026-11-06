from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from dsql_replicator.applier.autoscaler import AutoscaleParams, WorkerAutoscaler


def _params(**overrides) -> AutoscaleParams:
    defaults = dict(
        floor=128,
        ceiling=256,
        step=32,
        tick_s=0.02,  # 20 ms for fast tests
        scale_up_lag_ms=10_000.0,
        scale_up_sustain_s=0.06,  # need 3 samples above threshold
        scale_down_lag_ms=2_000.0,
        scale_down_sustain_s=0.06,
    )
    defaults.update(overrides)
    return AutoscaleParams(**defaults)


def _fake_applier(initial_workers: int = 128, ceiling: int = 256):
    a = MagicMock()
    a.worker_count = initial_workers
    a.max_worker_count = ceiling

    async def grow(delta: int) -> int:
        added = min(delta, ceiling - a.worker_count)
        a.worker_count += added
        return added

    async def shrink(delta: int) -> int:
        removed = min(delta, a.worker_count - 1)
        a.worker_count -= removed
        return removed

    a.grow_pool = AsyncMock(side_effect=grow)
    a.shrink_pool = AsyncMock(side_effect=shrink)
    return a


@pytest.mark.asyncio
async def test_disabled_when_ceiling_equals_floor() -> None:
    a = _fake_applier(128, 128)
    stop = asyncio.Event()
    sc = WorkerAutoscaler(a, lambda: 50_000.0, _params(ceiling=128), stop)
    # Should return immediately with no sampling / no grow calls.
    await asyncio.wait_for(sc.run(), timeout=0.2)
    a.grow_pool.assert_not_awaited()
    a.shrink_pool.assert_not_awaited()


@pytest.mark.asyncio
async def test_scales_up_on_sustained_lag() -> None:
    a = _fake_applier(128, 256)
    stop = asyncio.Event()
    sc = WorkerAutoscaler(a, lambda: 50_000.0, _params(), stop)
    task = asyncio.create_task(sc.run())
    await asyncio.sleep(0.25)  # several ticks past sustain window
    stop.set()
    await task
    assert a.worker_count > 128
    a.grow_pool.assert_awaited()


@pytest.mark.asyncio
async def test_no_scale_up_on_single_spike() -> None:
    # Lag spikes once then recovers; sustain window never fills with highs.
    samples = iter([50_000.0] + [500.0] * 100)
    a = _fake_applier(128, 256)
    stop = asyncio.Event()
    sc = WorkerAutoscaler(a, lambda: next(samples, 500.0), _params(), stop)
    task = asyncio.create_task(sc.run())
    await asyncio.sleep(0.2)
    stop.set()
    await task
    assert a.worker_count == 128
    a.grow_pool.assert_not_awaited()


@pytest.mark.asyncio
async def test_respects_ceiling() -> None:
    a = _fake_applier(240, 256)  # already near the ceiling
    stop = asyncio.Event()
    sc = WorkerAutoscaler(a, lambda: 50_000.0, _params(step=32), stop)
    task = asyncio.create_task(sc.run())
    await asyncio.sleep(0.3)
    stop.set()
    await task
    assert a.worker_count == 256  # grew to ceiling, no further


@pytest.mark.asyncio
async def test_scales_down_on_sustained_recovery() -> None:
    a = _fake_applier(224, 256)
    stop = asyncio.Event()
    sc = WorkerAutoscaler(a, lambda: 100.0, _params(), stop)
    task = asyncio.create_task(sc.run())
    await asyncio.sleep(0.25)
    stop.set()
    await task
    assert a.worker_count < 224
    assert a.worker_count >= 128  # does not go below floor
    a.shrink_pool.assert_awaited()


@pytest.mark.asyncio
async def test_respects_floor_on_shrink() -> None:
    a = _fake_applier(128, 256)
    stop = asyncio.Event()
    sc = WorkerAutoscaler(a, lambda: 100.0, _params(), stop)
    task = asyncio.create_task(sc.run())
    await asyncio.sleep(0.2)
    stop.set()
    await task
    assert a.worker_count == 128  # at floor, no shrink
    a.shrink_pool.assert_not_awaited()


@pytest.mark.asyncio
async def test_handles_missing_signal_gracefully() -> None:
    # watermark not advanced yet → None signal → scaler just loops.
    a = _fake_applier(128, 256)
    stop = asyncio.Event()
    sc = WorkerAutoscaler(a, lambda: None, _params(), stop)
    task = asyncio.create_task(sc.run())
    await asyncio.sleep(0.1)
    stop.set()
    await task
    assert a.worker_count == 128
    a.grow_pool.assert_not_awaited()
    a.shrink_pool.assert_not_awaited()
