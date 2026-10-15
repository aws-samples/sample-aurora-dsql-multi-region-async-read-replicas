"""CloudWatch dashboard build + upsert.

Mirrors the metrics actually emitted by `src/dsql_replicator/metrics.py` + the pipeline's
`_emit_metrics` method. All metrics share the dimension `TargetCluster`; per-stream and
per-shard metrics add `Stream` and `Shard` respectively. We don't pin a specific stream
in the dashboard — operators can filter in the console.
"""

from __future__ import annotations

import json
from typing import Any

from dsql_replicator.config import Config

from .report import ActionStatus, MigrateReport


def ensure_dashboard(cw: Any, cfg: Config, report: MigrateReport) -> None:
    body = _build_dashboard(cfg)
    name = f"{cfg.resource_prefix}-{cfg.target_cluster_id}"
    cw.put_dashboard(DashboardName=name, DashboardBody=json.dumps(body))
    report.record("cw_dashboard", name, ActionStatus.UPDATED, "put_dashboard ok")


def _build_dashboard(cfg: Config) -> dict[str, Any]:
    ns = cfg.metrics_namespace
    tgt = cfg.target_cluster_id
    region = cfg.control_region

    def _ts(
        title: str,
        metrics: list[list[Any]],
        y: int,
        x: int,
        w: int,
        h: int,
        stat: str = "Sum",
    ) -> dict[str, Any]:
        return {
            "type": "metric",
            "x": x,
            "y": y,
            "width": w,
            "height": h,
            "properties": {
                "title": title,
                "metrics": metrics,
                "region": region,
                "stat": stat,
                "period": 60,
                "view": "timeSeries",
                "stacked": False,
            },
        }

    def _sv(
        title: str,
        metrics: list[list[Any]],
        y: int,
        x: int,
        w: int,
        h: int,
        stat: str = "Maximum",
    ) -> dict[str, Any]:
        return {
            "type": "metric",
            "x": x,
            "y": y,
            "width": w,
            "height": h,
            "properties": {
                "title": title,
                "metrics": metrics,
                "region": region,
                "stat": stat,
                "period": 60,
                "view": "singleValue",
            },
        }

    def _m(name: str) -> list[Any]:
        return [ns, name, "TargetCluster", tgt]

    widgets: list[dict[str, Any]] = [
        # Row 1 — single-value snapshot: fence state + backlog + lag
        _sv("Open fence age (s)", [_m("OpenFenceAgeSec")], 0, 0, 6, 4),
        _sv("Open fence count", [_m("OpenFenceCount")], 0, 6, 6, 4),
        _sv("Ordering buffer depth", [_m("OrderingBufferDepth")], 0, 12, 6, 4),
        _sv("Watermark lag (ms)", [_m("WatermarkLagMs")], 0, 18, 6, 4, stat="Average"),
        # Row 2 — throughput
        _ts(
            "Throughput (per-min)",
            [
                _m("RecordsReceived"),
                _m("TransactionsCommitted"),
                _m("RowsReplicated"),
                _m("RowsDeduped"),
            ],
            4,
            0,
            24,
            6,
            stat="Sum",
        ),
        # Row 3 — errors + fence lifecycle
        _ts(
            "Errors & fence lifecycle",
            [
                _m("ApplyErrors"),
                _m("ParseErrors"),
                _m("FencesOpened"),
                _m("FencesClosedObserved"),
            ],
            10,
            0,
            24,
            6,
            stat="Sum",
        ),
        # Row 4 — lag + backlog over time
        _ts(
            "Watermark + backlog",
            [
                _m("WatermarkLagMs"),
                _m("OrderingBufferDepth"),
                _m("OrderingBufferHeadAgeMs"),
                _m("IdleShardSyntheticAdvances"),
            ],
            16,
            0,
            24,
            6,
            stat="Maximum",
        ),
        # Row 5 — leases + checkpoints
        _ts(
            "Lease + checkpoint activity",
            [
                _m("LeasesAcquired"),
                _m("LeasesLost"),
                _m("CheckpointsFlushed"),
                _m("FenceActive"),
            ],
            22,
            0,
            24,
            6,
            stat="Sum",
        ),
        # Row 6 — per-shard Kinesis lag (shard dim auto-expanded by CloudWatch when present)
        _ts(
            "Kinesis MillisBehindLatest (per shard)",
            [[ns, "KinesisMillisBehindLatest", "TargetCluster", tgt]],
            28,
            0,
            24,
            6,
            stat="Maximum",
        ),
    ]
    return {"widgets": widgets}
