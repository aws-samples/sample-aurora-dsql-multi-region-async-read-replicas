"""Unit tests for M6.0 stripe-locked parallelism.

Target the pure-Python pieces (stripe hashing + sorted lock acquisition). Live DSQL
behavior is covered by live-cluster throughput tests.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from dsql_replicator.applier.apply import _stripe_of


def test_stripe_hash_stable_across_calls() -> None:
    key = ("public", "demo_events", ("42",))
    s1 = _stripe_of(key, 64)
    s2 = _stripe_of(key, 64)
    assert s1 == s2
    assert 0 <= s1 < 64


def test_stripe_hash_differs_across_keys() -> None:
    keys = [("public", "demo_events", (str(i),)) for i in range(1000)]
    stripes = [_stripe_of(k, 64) for k in keys]
    # 1000 keys, 64 stripes — pigeonhole guarantees <=64 unique but we want decent spread.
    unique = len(set(stripes))
    assert unique >= 40, f"stripe hashing has poor spread: only {unique}/64 used"


def test_stripe_hash_respects_count() -> None:
    key = ("public", "t", ("x",))
    for n in (1, 2, 4, 16, 128):
        s = _stripe_of(key, n)
        assert 0 <= s < n


def test_stripe_hash_distinguishes_schema_table_pk() -> None:
    a = _stripe_of(("public", "t", ("1",)), 64)
    b = _stripe_of(("public", "t", ("2",)), 64)
    c = _stripe_of(("public", "u", ("1",)), 64)
    d = _stripe_of(("schema2", "t", ("1",)), 64)
    # At least some of these must differ — identical hash across all 4 is almost
    # certainly a bug in the hash function.
    assert len({a, b, c, d}) > 1


@pytest.mark.asyncio
async def test_sorted_lock_acquisition_deadlock_free() -> None:
    """Two tasks acquiring the same two locks in sorted order must not deadlock."""
    lock_a = asyncio.Lock()
    lock_b = asyncio.Lock()

    async def worker(order: list[asyncio.Lock], result: list[str], label: str) -> None:
        for lk in order:
            await lk.acquire()
        result.append(label)
        for lk in reversed(order):
            lk.release()

    result: list[str] = []
    # Both workers acquire [lock_a, lock_b] in the SAME order — sorted-order discipline.
    await asyncio.wait_for(
        asyncio.gather(
            worker([lock_a, lock_b], result, "w1"),
            worker([lock_a, lock_b], result, "w2"),
        ),
        timeout=2.0,
    )
    assert sorted(result) == ["w1", "w2"]


@pytest.mark.asyncio
async def test_disjoint_locks_run_in_parallel() -> None:
    """Workers on disjoint locks progress concurrently, not serially."""
    locks = [asyncio.Lock() for _ in range(4)]
    ran_at: list[tuple[str, float]] = []
    loop = asyncio.get_event_loop()

    async def worker(idx: int, lk: asyncio.Lock) -> None:
        async with lk:
            ran_at.append((f"w{idx}", loop.time()))
            await asyncio.sleep(0.05)

    t0 = loop.time()
    await asyncio.gather(*[worker(i, locks[i]) for i in range(4)])
    total = loop.time() - t0
    # Serial would take ~0.2s; parallel should be well under 0.1s.
    assert total < 0.15, f"disjoint locks serialized: total={total:.3f}s"
    assert len(ran_at) == 4


def test_mock_metric_in_apply_result() -> None:
    """ApplyResult carries the stripe tuple (used for logging + metric breakdowns)."""
    from dsql_replicator.applier.apply import ApplyResult

    r = ApplyResult(
        tx_id="t",
        rows_applied=1,
        rows_deduped=0,
        attempt=1,
        duration_ms=10.0,
        stripes=(3, 17, 55),
    )
    assert r.stripes == (3, 17, 55)
    # Ensure default is empty tuple so existing callers don't break.
    r2 = ApplyResult(tx_id="t", rows_applied=1, rows_deduped=0, attempt=1, duration_ms=1.0)
    assert r2.stripes == ()
    _ = MagicMock  # keep import for future expansion
