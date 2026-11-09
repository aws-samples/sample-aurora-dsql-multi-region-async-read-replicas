"""Env-driven runtime configuration. See src/dsql_replicator/config.py for the full list."""

from __future__ import annotations

import os
import socket
import uuid
from dataclasses import dataclass

from .errors import ConfigError


def _default_task_id() -> str:
    """Stable-ish task identifier for lease ownership. ECS tasks override via TASK_ID
    env (set to the ECS task ARN at CDK level); for local runs we use hostname+uuid."""
    try:
        host = socket.gethostname()
    except Exception:
        host = "unknown"
    return f"{host}-{uuid.uuid4().hex[:8]}"


def _required(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise ConfigError(f"missing required env var: {name}")
    return v


def _optional(name: str, default: str) -> str:
    return os.environ.get(name) or default


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"invalid int for {name}: {raw!r}") from exc




@dataclass(frozen=True)
class Config:
    # Source
    source_streams: tuple[str, ...]
    source_region: str
    # BYO source cluster: if set, the consumer auto-derives source_streams from
    # the cluster's CDC configuration at startup (see preflight.py). Supplying
    # this instead of SOURCE_STREAMS also verifies CDC is actually enabled.
    source_cluster_id: str | None

    # Target
    target_cluster_id: str
    target_region: str

    # State plane (post-2026-10-12: SNS + CloudWatch only — state is in target DSQL)
    resource_prefix: str
    sns_topic_arn: str | None
    control_region: str

    # Reassembly (fragment only — transaction reassembly removed in the 2026-11
    # simplification pass). `ordering_buffer_max_depth` is reused as the apply
    # queue bound so operator-set backpressure values keep working.
    fragment_timeout_ms: int
    ordering_buffer_max_depth: int

    # Apply
    apply_backoff_initial_ms: int
    checkpoint_flush_interval_ms: int

    # Shard leases (M4)
    task_id: str
    lease_ttl_seconds: int
    lease_heartbeat_seconds: int
    lease_steal_scan_seconds: int

    # Observability (M5)
    metrics_interval_ms: int

    # Shard discovery: how often to re-list Kinesis shards after startup so
    # that mid-run resharding (e.g. 10 shards → 12 shards) is picked up
    # without a task restart. Set to 0 to disable periodic re-discovery.
    shard_discovery_interval_ms: int

    # Per-key parallel apply (M6.0)
    applier_stripes: int
    applier_workers: int

    # Writer autoscaling: start at `applier_workers` (floor), grow to
    # `applier_workers_max` on sustained lag, shrink back on recovery.
    # Set applier_workers_max <= applier_workers to disable autoscaling.
    applier_workers_max: int
    applier_scale_step: int
    applier_scale_tick_ms: int
    applier_scale_up_lag_ms: int
    applier_scale_up_sustain_ms: int
    applier_scale_down_lag_ms: int
    applier_scale_down_sustain_ms: int

    # Micro-batching (A): pack multiple source txns into one target BEGIN...COMMIT.
    # Size-or-time flush. One cross-region commit amortizes N source txns' RTT.
    batch_max_txns: int
    batch_max_wait_ms: int

    # Multi-process (C): >1 spawns N child consumer processes (one asyncio loop each)
    # so the GIL stops being the single-task throughput ceiling. Lease arbitration
    # across children is the existing __replicator_shard_leases table.
    replicator_processes: int

    # Observability
    metrics_namespace: str

    @classmethod
    def from_env(cls) -> Config:
        source_cluster_id = os.environ.get("SOURCE_CLUSTER_ID") or None
        raw_streams = os.environ.get("SOURCE_STREAMS", "")
        source_streams = tuple(s.strip() for s in raw_streams.split(",") if s.strip())
        if not source_streams and not source_cluster_id:
            raise ConfigError(
                "either SOURCE_STREAMS or SOURCE_CLUSTER_ID must be set "
                "(SOURCE_CLUSTER_ID auto-derives the stream from the DSQL cluster's CDC config)"
            )

        source_region = _optional("SOURCE_REGION", "us-east-1")
        return cls(
            source_streams=source_streams,
            source_region=source_region,
            source_cluster_id=source_cluster_id,
            target_cluster_id=_required("TARGET_CLUSTER_ID"),
            target_region=_required("TARGET_REGION"),
            resource_prefix=_optional("RESOURCE_PREFIX", "dsql-replicator"),
            sns_topic_arn=os.environ.get("SNS_TOPIC_ARN") or None,
            control_region=_optional("CONTROL_REGION", source_region),
            fragment_timeout_ms=_int("FRAGMENT_TIMEOUT_MS", 120_000),
            ordering_buffer_max_depth=_int("ORDERING_BUFFER_MAX_DEPTH", 100_000),
            apply_backoff_initial_ms=_int("APPLY_BACKOFF_INITIAL_MS", 100),
            checkpoint_flush_interval_ms=_int("CHECKPOINT_FLUSH_INTERVAL_MS", 1_000),
            task_id=_optional("TASK_ID", _default_task_id()),
            lease_ttl_seconds=_int("LEASE_TTL_SECONDS", 30),
            lease_heartbeat_seconds=_int("LEASE_HEARTBEAT_SECONDS", 10),
            lease_steal_scan_seconds=_int("LEASE_STEAL_SCAN_SECONDS", 30),
            metrics_interval_ms=_int("METRICS_INTERVAL_MS", 10_000),
            shard_discovery_interval_ms=_int("SHARD_DISCOVERY_INTERVAL_MS", 300_000),
            applier_stripes=_int("APPLIER_STRIPES", 64),
            applier_workers=_int("APPLIER_WORKERS", 32),
            applier_workers_max=_int("APPLIER_WORKERS_MAX", 0),
            applier_scale_step=_int("APPLIER_SCALE_STEP", 32),
            applier_scale_tick_ms=_int("APPLIER_SCALE_TICK_MS", 10_000),
            applier_scale_up_lag_ms=_int("APPLIER_SCALE_UP_LAG_MS", 10_000),
            applier_scale_up_sustain_ms=_int("APPLIER_SCALE_UP_SUSTAIN_MS", 60_000),
            applier_scale_down_lag_ms=_int("APPLIER_SCALE_DOWN_LAG_MS", 2_000),
            applier_scale_down_sustain_ms=_int("APPLIER_SCALE_DOWN_SUSTAIN_MS", 300_000),
            batch_max_txns=_int("BATCH_MAX_TXNS", 16),
            batch_max_wait_ms=_int("BATCH_MAX_WAIT_MS", 50),
            replicator_processes=_int("REPLICATOR_PROCESSES", 1),
            metrics_namespace=_optional("METRICS_NAMESPACE", "DSQLReplicator"),
        )

    def __post_init__(self) -> None:
        if self.batch_max_txns < 1:
            raise ConfigError(f"BATCH_MAX_TXNS must be >= 1, got {self.batch_max_txns}")
        if self.batch_max_wait_ms < 0:
            raise ConfigError(f"BATCH_MAX_WAIT_MS must be >= 0, got {self.batch_max_wait_ms}")
        if self.replicator_processes < 1:
            raise ConfigError(f"REPLICATOR_PROCESSES must be >= 1, got {self.replicator_processes}")
