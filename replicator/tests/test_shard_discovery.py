from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from dsql_replicator.consumer.pipeline import ConsumerPipeline


class _FakePipeline:
    """Just enough shape to exercise _shard_discovery_loop in isolation,
    without wiring up the full ConsumerPipeline (which needs DSQL + Kinesis
    + an aioboto3 session). We bind the real method onto this stub."""

    def __init__(self, interval_ms: int, shards_by_poll: list[list[str]]):
        self._stop = asyncio.Event()
        self._stream_name = "test-stream"

        cfg = MagicMock()
        cfg.shard_discovery_interval_ms = interval_ms
        self._cfg = cfg

        self._shards_by_poll = shards_by_poll
        self._list_calls = 0

        self.spawned: list[str] = []

    async def _list_shards(self, _kinesis):
        if self._list_calls >= len(self._shards_by_poll):
            # After the scripted sequence, return the last state forever.
            idx = len(self._shards_by_poll) - 1
        else:
            idx = self._list_calls
        self._list_calls += 1
        return list(self._shards_by_poll[idx])

    def _spawn_worker(self, shard_id: str) -> asyncio.Task:
        self.spawned.append(shard_id)
        return asyncio.create_task(asyncio.sleep(10), name=f"shard:{shard_id}")

    # Bind the real method so we test the actual implementation.
    _shard_discovery_loop = ConsumerPipeline._shard_discovery_loop

    # The real loop calls self._shard_worker; redirect to our _spawn_worker.
    def _shard_worker(self, _kinesis, shard_id):
        # Not used — asyncio.create_task is called inside the loop with
        # self._shard_worker(..). Return a benign coroutine.
        async def noop():
            await asyncio.sleep(10)

        self.spawned.append(shard_id)
        return noop()


@pytest.mark.asyncio
async def test_disabled_when_interval_zero() -> None:
    p = _FakePipeline(interval_ms=0, shards_by_poll=[["s1"]])
    workers: dict[str, asyncio.Task] = {}
    # Should return immediately without polling.
    await asyncio.wait_for(p._shard_discovery_loop(MagicMock(), workers), timeout=0.5)
    assert p._list_calls == 0
    assert p.spawned == []


@pytest.mark.asyncio
async def test_detects_new_shards_on_second_poll() -> None:
    # First poll: s1, s2 (already known). Second poll: s1, s2, s3, s4 (two new).
    p = _FakePipeline(
        interval_ms=50,
        shards_by_poll=[["s1", "s2"], ["s1", "s2", "s3", "s4"]],
    )
    workers: dict[str, asyncio.Task] = {
        "s1": asyncio.create_task(asyncio.sleep(10)),
        "s2": asyncio.create_task(asyncio.sleep(10)),
    }
    loop_task = asyncio.create_task(p._shard_discovery_loop(MagicMock(), workers))
    await asyncio.sleep(0.15)  # ~3 poll intervals
    p._stop.set()
    await loop_task

    assert "s3" in workers
    assert "s4" in workers
    # s1/s2 were already present → should NOT have been respawned.
    assert sorted(p.spawned) == ["s3", "s4"]

    for t in workers.values():
        t.cancel()
    await asyncio.gather(*workers.values(), return_exceptions=True)


@pytest.mark.asyncio
async def test_prunes_done_workers_before_diffing() -> None:
    # Shard s1 is "known" but its worker finished (closed shard, lease loss).
    # On next poll, s1 is still reported → the loop should respawn it.
    async def finished():
        return None

    done_task = asyncio.create_task(finished())
    await done_task  # ensure it's actually done

    p = _FakePipeline(interval_ms=50, shards_by_poll=[["s1"]])
    workers: dict[str, asyncio.Task] = {"s1": done_task}

    loop_task = asyncio.create_task(p._shard_discovery_loop(MagicMock(), workers))
    await asyncio.sleep(0.1)
    p._stop.set()
    await loop_task

    assert p.spawned == ["s1"], "expected s1 to be respawned after its worker finished"
    for t in workers.values():
        if not t.done():
            t.cancel()
    await asyncio.gather(*workers.values(), return_exceptions=True)


@pytest.mark.asyncio
async def test_list_shards_error_does_not_crash_loop() -> None:
    class _BoomPipeline(_FakePipeline):
        async def _list_shards(self, _kinesis):
            self._list_calls += 1
            if self._list_calls == 1:
                raise RuntimeError("throttled")
            return ["s1"]

    p = _BoomPipeline(interval_ms=50, shards_by_poll=[[]])
    workers: dict[str, asyncio.Task] = {}
    loop_task = asyncio.create_task(p._shard_discovery_loop(MagicMock(), workers))
    await asyncio.sleep(0.15)
    p._stop.set()
    await loop_task

    assert p._list_calls >= 2
    assert "s1" in workers
    for t in workers.values():
        t.cancel()
    await asyncio.gather(*workers.values(), return_exceptions=True)
