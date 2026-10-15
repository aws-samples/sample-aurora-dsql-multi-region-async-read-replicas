"""Unit tests for the MetricsEmitter — focus on batching + shape, not CloudWatch itself."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dsql_replicator.metrics import MetricPoint, MetricsEmitter


@pytest.mark.asyncio
async def test_put_batch_empty_noop() -> None:
    emitter = MetricsEmitter(namespace="X", region="us-east-1", default_dims=(("TargetCluster", "c1"),))
    emitter._client = MagicMock()  # type: ignore[assignment]
    await emitter.put_batch([])
    emitter._client.put_metric_data.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_put_batch_single_call_shape() -> None:
    emitter = MetricsEmitter(namespace="X", region="us-east-1", default_dims=(("TargetCluster", "c1"),))
    client = MagicMock()
    emitter._client = client  # type: ignore[assignment]
    points = [
        MetricPoint("RecordsReceived", 7, "Count", (("Stream", "s1"),)),
        MetricPoint("WatermarkLagMs", 123.4, "Milliseconds", (("Stream", "s1"),)),
    ]
    await emitter.put_batch(points)
    assert client.put_metric_data.call_count == 1
    _, kwargs = client.put_metric_data.call_args
    assert kwargs["Namespace"] == "X"
    data = kwargs["MetricData"]
    assert len(data) == 2
    first = data[0]
    assert first["MetricName"] == "RecordsReceived"
    assert first["Value"] == 7.0
    assert first["Unit"] == "Count"
    # Default dim first, point dim second.
    assert first["Dimensions"] == [
        {"Name": "TargetCluster", "Value": "c1"},
        {"Name": "Stream", "Value": "s1"},
    ]


@pytest.mark.asyncio
async def test_put_batch_chunks_above_twenty() -> None:
    emitter = MetricsEmitter(namespace="X", region="us-east-1")
    client = MagicMock()
    emitter._client = client  # type: ignore[assignment]
    points = [MetricPoint(f"M{i}", float(i)) for i in range(45)]
    await emitter.put_batch(points)
    # 20 + 20 + 5 = 3 calls
    assert client.put_metric_data.call_count == 3


@pytest.mark.asyncio
async def test_put_batch_swallows_cloudwatch_error() -> None:
    emitter = MetricsEmitter(namespace="X", region="us-east-1")
    client = MagicMock()
    client.put_metric_data.side_effect = RuntimeError("boom")
    emitter._client = client  # type: ignore[assignment]
    # Should NOT raise.
    await emitter.put_batch([MetricPoint("A", 1.0)])
    assert client.put_metric_data.call_count == 1
