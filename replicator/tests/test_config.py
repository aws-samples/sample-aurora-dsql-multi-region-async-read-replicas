from __future__ import annotations

import pytest

from dsql_replicator.config import Config
from dsql_replicator.errors import ConfigError

_REQUIRED = {
    "SOURCE_STREAMS": "dsql-cdc-stream",
    "TARGET_CLUSTER_ID": "abc123",
    "TARGET_REGION": "us-west-2",
}


def _set_env(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
    for var in [
        "SOURCE_STREAMS",
        "SOURCE_REGION",
        "TARGET_CLUSTER_ID",
        "TARGET_REGION",
        "TARGET_ENDPOINT_URL",
        "RESOURCE_PREFIX",
        "SNS_TOPIC_ARN",
        "CONTROL_REGION",
        "TXN_ASSEMBLY_TIMEOUT_MS",
        "FRAGMENT_TIMEOUT_MS",
        "ORDERING_BUFFER_MAX_DEPTH",
        "WATERMARK_IDLE_SHARD_TIMEOUT_MS",
        "WATERMARK_CLOCK_SKEW_BUDGET_MS",
        "APPLY_MAX_RETRIES",
        "APPLY_BACKOFF_INITIAL_MS",
        "CHECKPOINT_FLUSH_INTERVAL_MS",
        "QUARANTINE_HARD_CAP",
        "DLQ_FENCE_PAUSE_ON_OPEN",
        "METRICS_NAMESPACE",
    ]:
        monkeypatch.delenv(var, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)


def test_minimum_viable_config(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch, _REQUIRED)
    cfg = Config.from_env()
    assert cfg.source_streams == ("dsql-cdc-stream",)
    assert cfg.target_cluster_id == "abc123"
    assert cfg.source_region == "us-east-1"
    assert cfg.apply_max_retries == 5
    assert cfg.resource_prefix == "dsql-replicator"


def test_parses_multiple_streams(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch, _REQUIRED | {"SOURCE_STREAMS": " a , b ,c"})
    cfg = Config.from_env()
    assert cfg.source_streams == ("a", "b", "c")


def test_rejects_missing_required(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch, {})
    with pytest.raises(ConfigError, match="SOURCE_STREAMS"):
        Config.from_env()


def test_rejects_bad_int(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch, _REQUIRED | {"APPLY_MAX_RETRIES": "not-a-number"})
    with pytest.raises(ConfigError, match="APPLY_MAX_RETRIES"):
        Config.from_env()


def test_rejects_bad_bool(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch, _REQUIRED | {"DLQ_FENCE_PAUSE_ON_OPEN": "maybe"})
    with pytest.raises(ConfigError, match="DLQ_FENCE_PAUSE_ON_OPEN"):
        Config.from_env()
