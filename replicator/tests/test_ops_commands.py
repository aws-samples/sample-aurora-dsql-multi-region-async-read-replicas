"""Ops CLI logic that doesn't need a live target DSQL — focus on dispatch + policy.

Live scenarios (actual retry, actual pause/unpause, fence poller handshake) are covered
by live-cluster test harnesses.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from dsql_replicator.cli import main


def _setenv(monkeypatch: pytest.MonkeyPatch, **extra: str) -> None:
    defaults = {
        "SOURCE_STREAMS": "dsql-cdc-stream",
        "SOURCE_REGION": "us-east-1",
        "TARGET_CLUSTER_ID": "abc123",
        "TARGET_REGION": "us-west-2",
    }
    for k, v in {**defaults, **extra}.items():
        monkeypatch.setenv(k, v)


def test_ops_help_lists_subcommands(monkeypatch: pytest.MonkeyPatch) -> None:
    _setenv(monkeypatch)
    r = CliRunner().invoke(main, ["ops", "--help"])
    assert r.exit_code == 0
    assert "dlq" in r.output
    assert "pause" in r.output
    assert "unpause" in r.output


def test_dlq_help_lists_subcommands(monkeypatch: pytest.MonkeyPatch) -> None:
    _setenv(monkeypatch)
    r = CliRunner().invoke(main, ["ops", "dlq", "--help"])
    assert r.exit_code == 0
    assert "list" in r.output
    assert "inspect" in r.output
    assert "retry" in r.output
    assert "skip" in r.output
    assert "edit-and-retry" in r.output


def test_dlq_skip_requires_confirm(monkeypatch: pytest.MonkeyPatch) -> None:
    _setenv(monkeypatch)
    r = CliRunner().invoke(main, ["ops", "dlq", "skip", "tx1"])
    assert r.exit_code != 0
    # click emits a usage error for missing required option.
    assert "--confirm" in r.output or "Missing" in r.output
