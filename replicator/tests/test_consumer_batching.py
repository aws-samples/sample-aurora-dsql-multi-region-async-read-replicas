"""Runner-side micro-batching tests.

Drives `ConsumerPipeline._apply_runner` against a fake `Applier` and a real
`OrderingBuffer`, verifying:

- Size trigger: when `batch_max_txns` txns are ready, they dispatch in one batch.
- Time trigger: a partial batch flushes after `batch_max_wait_ms` elapses.
- Fallback on batch failure: per-txn `apply()` runs; mid-batch failure opens
  fence at that txn and re-ingests the tail into the ordering buffer.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest

from dsql_replicator.applier.apply import ApplyError, ApplyResult
from dsql_replicator.consumer.pipeline import ConsumerPipeline
from dsql_replicator.envelope import FullRecord, parse
from dsql_replicator.ordering import OrderingBuffer
from dsql_replicator.reassembler import CompletedTransaction


def _full(tx_id: str, record_id: str, ts_ns: int) -> FullRecord:
    rec = parse(
        {
            "type": "full",
            "op": "c",
            "before": None,
            "after": {"id": record_id},
            "source": {
                "version": "1.1",
                "ts_ms": ts_ns // 1_000_000,
                "ts_ns": ts_ns,
                "txId": tx_id,
                "schema": "public",
                "table": "t",
                "db": "postgres",
                "cluster": "c",
            },
            "ts_ms": ts_ns // 1_000_000,
            "ts_ns": ts_ns,
            "transaction": {"total_records": 1, "record_id": record_id},
        }
    )
    assert isinstance(rec, FullRecord)
    return rec


def _txn(tx_id: str, ts_ns: int) -> CompletedTransaction:
    return CompletedTransaction(
        tx_id=tx_id, ts_ns=ts_ns, total_records=1, records=(_full(tx_id, "r0", ts_ns),)
    )


@dataclass
class _FakeApplier:
    """Replaces `ConsumerPipeline._applier` for runner-level tests. Captures
    which batches / individual txns the runner dispatched."""

    batches: list[list[CompletedTransaction]]
    singles: list[CompletedTransaction]
    fail_batch_once: bool = False
    fail_single_tx_id: str | None = None

    async def apply_batch(self, batch: list[CompletedTransaction]) -> list[ApplyResult]:
        self.batches.append(list(batch))
        if self.fail_batch_once:
            self.fail_batch_once = False
            raise ApplyError("test-induced batch failure")
        return [
            ApplyResult(tx_id=t.tx_id, rows_applied=1, rows_deduped=0, attempt=1, duration_ms=1.0)
            for t in batch
        ]

    async def apply(self, txn: CompletedTransaction) -> ApplyResult:
        self.singles.append(txn)
        if self.fail_single_tx_id is not None and txn.tx_id == self.fail_single_tx_id:
            raise ApplyError(f"test-induced single-txn failure on {txn.tx_id}")
        return ApplyResult(tx_id=txn.tx_id, rows_applied=1, rows_deduped=0, attempt=1, duration_ms=1.0)


def _bare_pipeline(batch_max_txns: int, batch_max_wait_ms: int) -> ConsumerPipeline:
    """Build a ConsumerPipeline with only the attributes `_apply_runner` touches."""
    cfg = MagicMock()
    cfg.applier_stripes = 2
    cfg.applier_workers = 4
    cfg.batch_max_txns = batch_max_txns
    cfg.batch_max_wait_ms = batch_max_wait_ms

    pipe = object.__new__(ConsumerPipeline)
    pipe._cfg = cfg  # type: ignore[attr-defined]
    pipe._stop = asyncio.Event()  # type: ignore[attr-defined]
    # Ordering buffer with one registered shard and a watermark that passes any ts_ns.
    buf = OrderingBuffer(
        idle_shard_timeout_seconds=0.0,
        clock_skew_budget_seconds=0.0,
        max_depth=1000,
    )
    buf.register_shard("test:0")
    pipe._ordering = buf  # type: ignore[attr-defined]
    # Counters touched by _apply_batch_task.
    pipe.transactions_committed = 0  # type: ignore[attr-defined]
    pipe.rows_replicated = 0  # type: ignore[attr-defined]
    pipe.rows_deduped = 0  # type: ignore[attr-defined]
    pipe.apply_errors = 0  # type: ignore[attr-defined]
    pipe.fences_opened = 0  # type: ignore[attr-defined]
    # _open_fence calls self._dlq.open_fence + self._ordering.set_fence; stub it.
    pipe._open_fence = AsyncMock()  # type: ignore[attr-defined]
    return pipe


def _publish(buf: OrderingBuffer, txn: CompletedTransaction) -> None:
    """Ingest a txn and advance the single shard's watermark past its ts_ns so
    `ready()` releases it on the next poll."""
    buf.ingest(txn)
    buf.update_shard("test:0", latest_ts_ns=txn.ts_ns + 1_000_000_000, millis_behind_latest=1)


@pytest.mark.asyncio
async def test_runner_size_trigger_dispatches_one_batch() -> None:
    pipe = _bare_pipeline(batch_max_txns=4, batch_max_wait_ms=10_000)
    fake = _FakeApplier(batches=[], singles=[])
    pipe._applier = fake  # type: ignore[attr-defined]
    for i in range(4):
        _publish(pipe._ordering, _txn(f"t{i}", ts_ns=100 + i))  # type: ignore[attr-defined]

    runner = asyncio.create_task(pipe._apply_runner())
    try:
        # Allow one scheduler tick for the size trigger to flush.
        for _ in range(10):
            await asyncio.sleep(0.02)
            if fake.batches:
                break
    finally:
        pipe._stop.set()  # type: ignore[attr-defined]
        await asyncio.wait_for(runner, timeout=2.0)

    assert len(fake.batches) == 1
    assert [t.tx_id for t in fake.batches[0]] == ["t0", "t1", "t2", "t3"]
    assert pipe.transactions_committed == 4  # type: ignore[attr-defined]
    assert not fake.singles


@pytest.mark.asyncio
async def test_runner_time_trigger_flushes_partial_batch() -> None:
    pipe = _bare_pipeline(batch_max_txns=16, batch_max_wait_ms=60)
    fake = _FakeApplier(batches=[], singles=[])
    pipe._applier = fake  # type: ignore[attr-defined]
    # Only 2 txns — not enough to hit size trigger.
    _publish(pipe._ordering, _txn("t0", ts_ns=100))  # type: ignore[attr-defined]
    _publish(pipe._ordering, _txn("t1", ts_ns=200))  # type: ignore[attr-defined]

    runner = asyncio.create_task(pipe._apply_runner())
    try:
        for _ in range(20):
            await asyncio.sleep(0.02)
            if fake.batches:
                break
    finally:
        pipe._stop.set()  # type: ignore[attr-defined]
        await asyncio.wait_for(runner, timeout=2.0)

    assert len(fake.batches) == 1
    assert len(fake.batches[0]) == 2
    assert pipe.transactions_committed == 2  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_runner_fallback_on_batch_failure_opens_fence_and_reingests_tail() -> None:
    """On batch failure the runner must: (a) retry each txn as single-txn apply,
    (b) commit the ones that succeed, (c) open a fence on the failing txn,
    (d) re-ingest txns after the failure into the ordering buffer."""
    pipe = _bare_pipeline(batch_max_txns=3, batch_max_wait_ms=10_000)
    fake = _FakeApplier(
        batches=[],
        singles=[],
        fail_batch_once=True,
        fail_single_tx_id="t1",  # middle txn is the bad one
    )
    pipe._applier = fake  # type: ignore[attr-defined]
    for i in range(3):
        _publish(pipe._ordering, _txn(f"t{i}", ts_ns=100 + i))  # type: ignore[attr-defined]

    runner = asyncio.create_task(pipe._apply_runner())
    try:
        for _ in range(50):
            await asyncio.sleep(0.02)
            if pipe._open_fence.await_count:  # type: ignore[attr-defined]
                break
    finally:
        pipe._stop.set()  # type: ignore[attr-defined]
        await asyncio.wait_for(runner, timeout=2.0)

    # One batch attempt that failed, then per-txn fallback picking up t0, t1.
    assert len(fake.batches) == 1
    assert [t.tx_id for t in fake.singles] == ["t0", "t1"]
    assert pipe.transactions_committed == 1  # type: ignore[attr-defined]  # only t0 committed
    assert pipe.apply_errors == 1  # type: ignore[attr-defined]
    pipe._open_fence.assert_awaited_once()  # type: ignore[attr-defined]
    # t2 was pushed back into the ordering buffer for post-fence replay.
    # Depth should be >= 1 after the test since the fence halted drain.
    assert pipe._ordering.depth() >= 1  # type: ignore[attr-defined]
