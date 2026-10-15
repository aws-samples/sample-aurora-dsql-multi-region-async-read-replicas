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


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    v = raw.strip().lower()
    if v in {"true", "1", "yes", "on"}:
        return True
    if v in {"false", "0", "no", "off"}:
        return False
    raise ConfigError(f"invalid bool for {name}: {raw!r}")


@dataclass(frozen=True)
class Config:
    # Source
    source_streams: tuple[str, ...]
    source_region: str

    # Target
    target_cluster_id: str
    target_region: str
    target_endpoint_url: str | None

    # State plane (post-2026-10-12: SNS + CloudWatch only — state is in target DSQL)
    resource_prefix: str
    sns_topic_arn: str | None
    control_region: str

    # Reassembly / ordering (milliseconds)
    txn_assembly_timeout_ms: int
    fragment_timeout_ms: int
    ordering_buffer_max_depth: int
    watermark_idle_shard_timeout_ms: int
    watermark_clock_skew_budget_ms: int

    # Apply
    apply_max_retries: int
    apply_backoff_initial_ms: int
    checkpoint_flush_interval_ms: int

    # DLQ
    quarantine_hard_cap: int
    dlq_fence_pause_on_open: bool
    dlq_sns_topic_name: str
    fence_poll_interval_ms: int
    require_skip_second_approval: bool

    # Shard leases (M4)
    task_id: str
    lease_ttl_seconds: int
    lease_heartbeat_seconds: int
    lease_steal_scan_seconds: int

    # Observability (M5)
    metrics_interval_ms: int

    # Per-key parallel apply (M6.0)
    applier_stripes: int
    applier_workers: int

    # Observability
    metrics_namespace: str

    @classmethod
    def from_env(cls) -> Config:
        source_streams = tuple(s.strip() for s in _required("SOURCE_STREAMS").split(",") if s.strip())
        if not source_streams:
            raise ConfigError("SOURCE_STREAMS must contain at least one stream")

        source_region = _optional("SOURCE_REGION", "us-east-1")
        return cls(
            source_streams=source_streams,
            source_region=source_region,
            target_cluster_id=_required("TARGET_CLUSTER_ID"),
            target_region=_required("TARGET_REGION"),
            target_endpoint_url=os.environ.get("TARGET_ENDPOINT_URL") or None,
            resource_prefix=_optional("RESOURCE_PREFIX", "dsql-replicator"),
            sns_topic_arn=os.environ.get("SNS_TOPIC_ARN") or None,
            control_region=_optional("CONTROL_REGION", source_region),
            txn_assembly_timeout_ms=_int("TXN_ASSEMBLY_TIMEOUT_MS", 60_000),
            fragment_timeout_ms=_int("FRAGMENT_TIMEOUT_MS", 120_000),
            ordering_buffer_max_depth=_int("ORDERING_BUFFER_MAX_DEPTH", 100_000),
            watermark_idle_shard_timeout_ms=_int("WATERMARK_IDLE_SHARD_TIMEOUT_MS", 30_000),
            watermark_clock_skew_budget_ms=_int("WATERMARK_CLOCK_SKEW_BUDGET_MS", 5_000),
            apply_max_retries=_int("APPLY_MAX_RETRIES", 5),
            apply_backoff_initial_ms=_int("APPLY_BACKOFF_INITIAL_MS", 100),
            checkpoint_flush_interval_ms=_int("CHECKPOINT_FLUSH_INTERVAL_MS", 1_000),
            quarantine_hard_cap=_int("QUARANTINE_HARD_CAP", 10_000),
            dlq_fence_pause_on_open=_bool("DLQ_FENCE_PAUSE_ON_OPEN", default=True),
            dlq_sns_topic_name=_optional("DLQ_SNS_TOPIC_NAME", "dsql-replicator-dlq-events"),
            fence_poll_interval_ms=_int("FENCE_POLL_INTERVAL_MS", 5_000),
            require_skip_second_approval=_bool("REQUIRE_SKIP_SECOND_APPROVAL", default=False),
            task_id=_optional("TASK_ID", _default_task_id()),
            lease_ttl_seconds=_int("LEASE_TTL_SECONDS", 30),
            lease_heartbeat_seconds=_int("LEASE_HEARTBEAT_SECONDS", 10),
            lease_steal_scan_seconds=_int("LEASE_STEAL_SCAN_SECONDS", 30),
            metrics_interval_ms=_int("METRICS_INTERVAL_MS", 10_000),
            applier_stripes=_int("APPLIER_STRIPES", 64),
            applier_workers=_int("APPLIER_WORKERS", 32),
            metrics_namespace=_optional("METRICS_NAMESPACE", "DSQLReplicator"),
        )
