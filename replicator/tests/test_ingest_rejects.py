"""Ingest-side reject routing (review issue #2).

The pipeline used to drop parse errors, CRC mismatches, fragment-reassembly
timeouts, and transaction-reassembly timeouts with only a log line. These are
now archived to `__replicator_dlq_ingest` for forensics and surfaced on
dedicated metrics so silent drops are no longer silent.

Target-DSQL interaction is covered by schema tests; here we unit-test the
pipeline bookkeeping: the counter advances, the DlqWriter is called with the
right reason tag, and failures to write to DSQL don't wedge the ingest loop.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dsql_replicator.consumer.pipeline import ConsumerPipeline
from dsql_replicator.dlq.schema import INGEST_REJECTS_DDL


def _bare_pipeline() -> ConsumerPipeline:
    pipe = object.__new__(ConsumerPipeline)
    pipe._stream_name = "test-stream"  # type: ignore[attr-defined]
    pipe._dlq = MagicMock()  # type: ignore[attr-defined]
    pipe._dlq.record_ingest_reject = AsyncMock(return_value="00000000-0000-0000-0000-000000000001")
    pipe.ingest_rejects_recorded = 0  # type: ignore[attr-defined]
    return pipe


@pytest.mark.asyncio
async def test_record_ingest_reject_calls_dlq_and_counts() -> None:
    pipe = _bare_pipeline()
    await pipe._record_ingest_reject(  # type: ignore[attr-defined]
        reason="parse_error",
        shard_id="shard-A",
        error_message="malformed envelope",
        raw_bytes=b"\x00\x01",
    )
    pipe._dlq.record_ingest_reject.assert_awaited_once()  # type: ignore[attr-defined]
    kwargs = pipe._dlq.record_ingest_reject.await_args.kwargs  # type: ignore[attr-defined]
    assert kwargs["stream_name"] == "test-stream"
    assert kwargs["reason"] == "parse_error"
    assert kwargs["shard_id"] == "shard-A"
    assert kwargs["raw_bytes"] == b"\x00\x01"
    assert pipe.ingest_rejects_recorded == 1  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_record_ingest_reject_swallows_errors() -> None:
    """A transient target-DSQL write failure MUST NOT wedge the ingest loop.
    The CloudWatch metric is the primary operator signal; the row is forensics."""
    pipe = _bare_pipeline()
    pipe._dlq.record_ingest_reject = AsyncMock(side_effect=RuntimeError("boom"))  # type: ignore[attr-defined]
    # Must not raise.
    await pipe._record_ingest_reject(reason="txn_timeout", tx_id="tx-999")  # type: ignore[attr-defined]
    assert pipe.ingest_rejects_recorded == 0  # type: ignore[attr-defined]


def test_ingest_rejects_ddl_mentions_reason_and_raw_bytes() -> None:
    """Schema sanity-check: structural guard against a column being dropped
    from `__replicator_dlq_ingest`. Each column is used by the writer or by
    operator forensics; renaming requires a migration we don't have yet."""
    for col in (
        "reject_id",
        "stream_name",
        "shard_id",
        "reason",
        "tx_id",
        "record_id",
        "chunk_id",
        "error_message",
        "raw_bytes",
        "details_json",
        "recorded_at",
    ):
        assert col in INGEST_REJECTS_DDL
