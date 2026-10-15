from __future__ import annotations

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from moto import mock_aws  # noqa: E402

from dsql_replicator.config import Config  # noqa: E402
from dsql_replicator.migrate import ActionStatus, run  # noqa: E402


def _cfg(monkeypatch: pytest.MonkeyPatch) -> Config:
    for k, v in {
        "SOURCE_STREAMS": "dsql-cdc-stream",
        "SOURCE_REGION": "us-east-1",
        "TARGET_CLUSTER_ID": "abc123",
        "TARGET_REGION": "us-west-2",
        "CONTROL_REGION": "us-east-1",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
    }.items():
        monkeypatch.setenv(k, v)
    return Config.from_env()


@mock_aws
def test_first_run_creates_everything(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch)
    report = run(cfg)
    by_kind: dict[str, list[ActionStatus]] = {}
    for a in report.actions:
        by_kind.setdefault(a.kind, []).append(a.status)

    # Post-M6.4: CDK owns SNS topics; migrate only upserts the CloudWatch dashboard.
    assert by_kind["cw_dashboard"] == [ActionStatus.UPDATED]
    assert "ddb_table" not in by_kind
    assert "sns_topic" not in by_kind


@mock_aws
def test_second_run_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _cfg(monkeypatch)
    run(cfg)
    report = run(cfg)
    kinds = [(a.kind, a.status) for a in report.actions]
    assert kinds.count(("cw_dashboard", ActionStatus.UPDATED)) == 1
    assert not any(k == "ddb_table" for k, _ in kinds)
    assert not any(k == "sns_topic" for k, _ in kinds)
