"""CloudWatch metrics emitter — thin wrapper over `boto3 cloudwatch put_metric_data`.

Namespace: `cfg.metrics_namespace` (default `DSQLReplicator`).

Dimensions attached to every metric:
- `TargetCluster` — the DSQL cluster identifier the replicator is writing to.
Per-stream metrics add `Stream`; per-shard metrics add `Stream` + `Shard`.

Design:
- Batched: each `put_batch` call emits up to 20 datapoints in one PutMetricData.
- Fire-and-forget: failures are logged + swallowed. Metrics loss ≠ apply loss.
- Synchronous `boto3` client wrapped in `asyncio.to_thread` so it never blocks the
  event loop. Call frequency is 1x per `METRICS_INTERVAL_MS` (default 10000) so the
  thread overhead is negligible.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import boto3
import structlog

log = structlog.get_logger("metrics")

MetricUnit = Literal[
    "Count",
    "Milliseconds",
    "Seconds",
    "None",
    "Bytes",
    "Percent",
]


@dataclass(frozen=True)
class MetricPoint:
    name: str
    value: float
    unit: MetricUnit = "Count"
    dims: tuple[tuple[str, str], ...] = ()


class MetricsEmitter:
    """Batches CloudWatch metric emissions. One emitter per process."""

    def __init__(
        self,
        *,
        namespace: str,
        region: str,
        default_dims: tuple[tuple[str, str], ...] = (),
    ) -> None:
        self._namespace = namespace
        self._default_dims = default_dims
        self._client = boto3.client("cloudwatch", region_name=region)

    async def put_batch(self, points: list[MetricPoint]) -> None:
        """Emit up to 20 metrics in one PutMetricData call; chunk further if needed."""
        if not points:
            return
        now = datetime.now(UTC)
        for chunk_start in range(0, len(points), 20):
            chunk = points[chunk_start : chunk_start + 20]
            data = [self._as_metric(p, now) for p in chunk]
            try:
                await asyncio.to_thread(
                    self._client.put_metric_data,
                    Namespace=self._namespace,
                    MetricData=data,
                )
            except Exception as exc:
                log.warning("metrics.put.failed", namespace=self._namespace, count=len(chunk), error=str(exc))

    def _as_metric(self, p: MetricPoint, ts: datetime) -> dict[str, Any]:
        dims = list(self._default_dims) + list(p.dims)
        return {
            "MetricName": p.name,
            "Value": float(p.value),
            "Unit": p.unit,
            "Timestamp": ts,
            "Dimensions": [{"Name": k, "Value": v} for k, v in dims],
        }
