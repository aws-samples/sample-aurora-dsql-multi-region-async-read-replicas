"""CloudWatch dashboard build + upsert.

Mirrors the metrics actually emitted by `src/dsql_replicator/metrics.py` + the pipeline's
`_emit_metrics` method. Metrics are emitted with dimension sets of either
`{TargetCluster, Stream}` or `{TargetCluster, Stream, ProcessId}` — the latter when
running under the multi-process supervisor (`REPLICATOR_PROCESSES > 1`).

Widgets use CloudWatch SEARCH expressions so a widget matches BOTH the single-process
emission (no ProcessId) and the multi-process emission (ProcessId=0, 1, ...) in one
place, aggregated. This avoids the "dashboard looks empty after enabling multi-process"
dim-mismatch footgun (incident 2026-10-26).
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
    # Pin the first configured stream to the dashboard. Operators running >1
    # stream per process should duplicate the dashboard per stream.
    stream = cfg.source_streams[0] if cfg.source_streams else "*"

    def _search(
        metric_name: str,
        agg: str = "SUM",
        *,
        inner_stat: str | None = None,
        label: str | None = None,
        extra_dim: str | None = None,
        extra_val: str | None = None,
    ) -> list[Any]:
        """Build a SEARCH-expression metric entry that aggregates across whatever
        ProcessId dims exist (0, 1, …, or absent for single-process).

        `agg` is the OUTER aggregator across process-series (SUM, MAX, AVG, MIN).
        `inner_stat` is the SEARCH's own per-series statistic over the period
        (defaults: counters → 'Sum', others → 'Average'). Choose 'Sum' when the
        metric is emitted as a per-interval delta; 'Average' when it is a gauge
        sampled each interval.
        """
        schema_dims = "TargetCluster,Stream,ProcessId"
        if extra_dim and extra_dim not in schema_dims.split(","):
            schema_dims = f"{schema_dims},{extra_dim}"
        filters = f'TargetCluster="{tgt}" Stream="{stream}" MetricName="{metric_name}"'
        if extra_dim and extra_val is not None:
            filters += f' {extra_dim}="{extra_val}"'
        if inner_stat is None:
            # Counter emission names end with these words; everything else is a gauge.
            counter_suffixes = (
                "Received",
                "Committed",
                "Replicated",
                "Deduped",
                "Discarded",
                "Errors",
                "Opened",
                "Observed",
                "Acquired",
                "Lost",
                "Flushed",
                "Timeouts",
                "Advances",
            )
            inner_stat = "Sum" if any(metric_name.endswith(s) for s in counter_suffixes) else "Average"
        expr = f"{agg}(SEARCH('{{{ns},{schema_dims}}} {filters}', '{inner_stat}', 60))"
        return [{"expression": expr, "label": label or metric_name, "id": _safe_id(metric_name)}]

    def _ts(
        title: str,
        metric_entries: list[list[Any]],
        y: int,
        x: int,
        w: int,
        h: int,
    ) -> dict[str, Any]:
        # Each SEARCH entry is itself the sub-array [{expression:...}] expected by
        # CloudWatch — pass through as-is; do NOT flatten.
        flat_metrics: list[Any] = list(metric_entries)
        return {
            "type": "metric",
            "x": x,
            "y": y,
            "width": w,
            "height": h,
            "properties": {
                "title": title,
                "metrics": flat_metrics,
                "region": region,
                "period": 60,
                "view": "timeSeries",
                "stacked": False,
            },
        }

    def _sv(
        title: str,
        metric_entries: list[list[Any]],
        y: int,
        x: int,
        w: int,
        h: int,
    ) -> dict[str, Any]:
        flat_metrics: list[Any] = list(metric_entries)
        return {
            "type": "metric",
            "x": x,
            "y": y,
            "width": w,
            "height": h,
            "properties": {
                "title": title,
                "metrics": flat_metrics,
                "region": region,
                "period": 60,
                "view": "singleValue",
            },
        }

    widgets: list[dict[str, Any]] = [
        # Row 1 — single-value snapshot: fence state + backlog + lag
        _sv(
            "Open fence age (s)",
            [_search("OpenFenceAgeSec", agg="MAX", label="Age (max across processes)")],
            0,
            0,
            6,
            4,
        ),
        _sv(
            "Open fence count",
            [_search("OpenFenceCount", agg="SUM", label="Fences (sum)")],
            0,
            6,
            6,
            4,
        ),
        _sv(
            "Ordering buffer depth",
            [_search("OrderingBufferDepth", agg="SUM", label="Buffered txns (sum)")],
            0,
            12,
            6,
            4,
        ),
        _sv(
            "Watermark lag (ms)",
            [_search("WatermarkLagMs", agg="MAX", label="Lag (max across processes)")],
            0,
            18,
            6,
            4,
        ),
        # Row 2 — throughput
        _ts(
            "Throughput (per-min, summed across processes)",
            [
                _search("RecordsReceived", agg="SUM", label="RecordsReceived"),
                _search("TransactionsCommitted", agg="SUM", label="TransactionsCommitted"),
                _search("RowsReplicated", agg="SUM", label="RowsReplicated"),
                _search("RowsDeduped", agg="SUM", label="RowsDeduped"),
                _search("RowsLateDiscarded", agg="SUM", label="RowsLateDiscarded"),
            ],
            4,
            0,
            24,
            6,
        ),
        # Row 3 — errors + fence lifecycle
        _ts(
            "Errors & fence lifecycle",
            [
                _search("ApplyErrors", agg="SUM", label="ApplyErrors"),
                _search("ParseErrors", agg="SUM", label="ParseErrors"),
                _search("FencesOpened", agg="SUM", label="FencesOpened"),
                _search("FencesClosedObserved", agg="SUM", label="FencesClosedObserved"),
                _search("TxnTimeouts", agg="SUM", label="TxnTimeouts"),
            ],
            10,
            0,
            24,
            6,
        ),
        # Row 4 — lag + backlog over time
        _ts(
            "Watermark + backlog",
            [
                _search("WatermarkLagMs", agg="MAX", label="WatermarkLagMs (max)"),
                _search("OrderingBufferDepth", agg="SUM", label="OrderingBufferDepth (sum)"),
                _search("OrderingBufferHeadAgeMs", agg="MAX", label="HeadAge (max)"),
                _search("IdleShardSyntheticAdvances", agg="SUM", label="IdleSynth (sum)"),
            ],
            16,
            0,
            24,
            6,
        ),
        # Row 5 — leases + checkpoints
        _ts(
            "Lease + checkpoint activity",
            [
                _search("LeasesAcquired", agg="SUM", label="LeasesAcquired"),
                _search("LeasesLost", agg="SUM", label="LeasesLost"),
                _search("CheckpointsFlushed", agg="SUM", label="CheckpointsFlushed"),
                _search("FenceActive", agg="MAX", label="FenceActive (max)"),
            ],
            22,
            0,
            24,
            6,
        ),
        # Row 6 — per-shard Kinesis lag; add Shard dim to schema so it enumerates.
        _ts(
            "Kinesis MillisBehindLatest (per shard, max across processes)",
            [
                _search(
                    "KinesisMillisBehindLatest",
                    agg="MAX",
                    label="Per-shard max",
                    extra_dim="Shard",
                    extra_val=None,
                )
            ],
            28,
            0,
            24,
            6,
        ),
    ]
    return {"widgets": widgets}


def _safe_id(name: str) -> str:
    """CloudWatch widget metric ids must match [a-z][a-zA-Z0-9]*."""
    cleaned = "".join(c for c in name if c.isalnum()) or "m"
    return "m" + cleaned[0].lower() + cleaned[1:]
