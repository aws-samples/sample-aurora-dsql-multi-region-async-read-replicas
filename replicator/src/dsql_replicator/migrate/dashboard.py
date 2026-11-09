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
        # CloudWatch SEARCH schema requires EXACT dim-set match. Single-process
        # emission omits `ProcessId`; multi-process emission includes it. One
        # SEARCH can't match both. We emit two parallel SEARCH entries — one
        # per schema shape — so each widget renders whichever shape produced
        # data this run. The empty one plots as a flat zero line and is
        # harmless.
        sp_schema = "TargetCluster,Stream"
        mp_schema = "TargetCluster,Stream,ProcessId"
        if extra_dim and extra_dim not in sp_schema.split(","):
            sp_schema = f"{sp_schema},{extra_dim}"
            mp_schema = f"{mp_schema},{extra_dim}"
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
                "Mismatches",
                "Recorded",
            )
            inner_stat = "Sum" if any(metric_name.endswith(s) for s in counter_suffixes) else "Average"
        sp_expr = f"{agg}(SEARCH('{{{ns},{sp_schema}}} {filters}', '{inner_stat}', 60))"
        mp_expr = f"{agg}(SEARCH('{{{ns},{mp_schema}}} {filters}', '{inner_stat}', 60))"
        safe = _safe_id(metric_name)
        # Each sub-array is one metric-math spec; the widget renders both as
        # separate series. The empty schema plots as flat zero (harmless).
        return [
            [{"expression": sp_expr, "label": (label or metric_name), "id": f"{safe}_sp"}],
            [{"expression": mp_expr, "label": f"{label or metric_name} (mp)", "id": f"{safe}_mp"}],
        ]

    def _ts(
        title: str,
        metric_entries: list[list[Any]],
        y: int,
        x: int,
        w: int,
        h: int,
    ) -> dict[str, Any]:
        # Each `_search(...)` returns a list of sub-arrays — one sub-array per
        # schema shape (single-process / multi-process). CloudWatch's widget
        # schema requires each entry in `metrics` to itself be a sub-array
        # (`[{expression,label,id}]`), so we flatten exactly one level deep.
        flat_metrics: list[Any] = []
        for entry in metric_entries:
            if isinstance(entry, list) and entry and isinstance(entry[0], list):
                # Nested: a list of sub-arrays — spread.
                flat_metrics.extend(entry)
            else:
                # Already a single sub-array.
                flat_metrics.append(entry)
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
        # Same one-level flatten as `_ts`: `_search` returns a list of
        # sub-arrays and each needs to appear as its own entry in `metrics`.
        flat_metrics: list[Any] = []
        for entry in metric_entries:
            if isinstance(entry, list) and entry and isinstance(entry[0], list):
                flat_metrics.extend(entry)
            else:
                flat_metrics.append(entry)
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
        # Row 1 — single-value snapshot: queue depth + replication lag
        _sv(
            "Apply queue depth",
            [_search("ApplyQueueDepth", agg="SUM", label="Depth (sum across processes)")],
            0,
            0,
            6,
            4,
        ),
        _sv(
            "Replication lag (ms)",
            [_search("ReplicationLagMs", agg="MAX", label="Lag (max across processes)")],
            0,
            6,
            6,
            4,
        ),
        # Row 2 — throughput
        _ts(
            "Throughput (per-min, summed across processes)",
            [
                _search("RecordsReceived", agg="SUM", label="RecordsReceived"),
                _search("ApplyBatches", agg="SUM", label="ApplyBatches"),
                _search("RowsApplied", agg="SUM", label="RowsApplied"),
                _search("RowsDiscarded", agg="SUM", label="RowsDiscarded"),
            ],
            4,
            0,
            24,
            6,
        ),
        # Row 3 — per-reason ingest errors so an operator can tell a parse_error
        # drop from a crc_mismatch drop from a fragment_timeout drop.
        _ts(
            "Ingest errors",
            [
                _search("ParseErrors", agg="SUM", label="ParseErrors"),
                _search("CrcMismatches", agg="SUM", label="CrcMismatches"),
                _search("FragmentTimeouts", agg="SUM", label="FragmentTimeouts"),
            ],
            10,
            0,
            24,
            6,
        ),
        # Row 4 — replication lag over time
        _ts(
            "Replication lag + queue depth",
            [
                _search("ReplicationLagMs", agg="MAX", label="ReplicationLagMs (max)"),
                _search("ApplyQueueDepth", agg="SUM", label="ApplyQueueDepth (sum)"),
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
