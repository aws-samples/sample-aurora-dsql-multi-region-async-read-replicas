"""Multi-process (pattern C) wiring tests — pure Python, no live DSQL.

Covers:
- `_liveness_path` picks per-process path when PROCESS_ID is set.
- healthz liveness probe aggregates across N child files: fails if ANY missing
  or stale, passes only when ALL fresh.
- Config knob plumbs through from env and rejects bad values.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from dsql_replicator.config import Config
from dsql_replicator.errors import ConfigError

# ---- Config knob ----------------------------------------------------------


def _base_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOURCE_STREAMS", "s1")
    monkeypatch.setenv("TARGET_CLUSTER_ID", "cid")
    monkeypatch.setenv("TARGET_REGION", "us-west-2")


def test_replicator_processes_defaults_to_one(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    cfg = Config.from_env()
    assert cfg.replicator_processes == 1


def test_replicator_processes_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    monkeypatch.setenv("REPLICATOR_PROCESSES", "4")
    cfg = Config.from_env()
    assert cfg.replicator_processes == 4


def test_replicator_processes_rejects_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    _base_env(monkeypatch)
    monkeypatch.setenv("REPLICATOR_PROCESSES", "0")
    with pytest.raises(ConfigError, match="REPLICATOR_PROCESSES"):
        Config.from_env()


# ---- Liveness path --------------------------------------------------------


def test_liveness_path_without_process_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PROCESS_ID", raising=False)
    from dsql_replicator.consumer.pipeline import _liveness_path

    assert _liveness_path() == "/tmp/dsql-replicator-liveness"


def test_liveness_path_with_process_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROCESS_ID", "3")
    from dsql_replicator.consumer.pipeline import _liveness_path

    assert _liveness_path() == "/tmp/dsql-replicator-liveness-3"


# ---- healthz multi-child liveness probe -----------------------------------


def _touch(p: Path, age_s: float = 0.0) -> None:
    p.touch(exist_ok=True)
    if age_s > 0:
        past = time.time() - age_s
        os.utime(p, (past, past))


def test_probe_liveness_single_process_passes_fresh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    live = tmp_path / "live"
    _touch(live)
    monkeypatch.setattr("dsql_replicator.health.LIVENESS_PATH", str(live))
    monkeypatch.delenv("REPLICATOR_PROCESSES", raising=False)
    from dsql_replicator.health import _probe_liveness

    r = _probe_liveness(None)  # type: ignore[arg-type]
    assert r.ok, r.detail


def test_probe_liveness_multi_child_passes_when_all_fresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "live"
    for i in range(3):
        _touch(Path(f"{base}-{i}"))
    monkeypatch.setattr("dsql_replicator.health.LIVENESS_PATH", str(base))
    monkeypatch.setenv("REPLICATOR_PROCESSES", "3")
    from dsql_replicator.health import _probe_liveness

    r = _probe_liveness(None)  # type: ignore[arg-type]
    assert r.ok, r.detail
    assert "all 3 children fresh" in r.detail


def test_probe_liveness_multi_child_fails_on_missing_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "live"
    _touch(Path(f"{base}-0"))
    # p1, p2 missing
    monkeypatch.setattr("dsql_replicator.health.LIVENESS_PATH", str(base))
    monkeypatch.setenv("REPLICATOR_PROCESSES", "3")
    from dsql_replicator.health import _probe_liveness

    r = _probe_liveness(None)  # type: ignore[arg-type]
    assert not r.ok
    assert "p1" in r.detail
    assert "p2" in r.detail


def test_probe_liveness_multi_child_fails_on_stale_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "live"
    _touch(Path(f"{base}-0"))
    _touch(Path(f"{base}-1"), age_s=600)  # stale beyond LIVENESS_MAX_AGE_SEC
    monkeypatch.setattr("dsql_replicator.health.LIVENESS_PATH", str(base))
    monkeypatch.setattr("dsql_replicator.health.LIVENESS_MAX_AGE_SEC", 60)
    monkeypatch.setenv("REPLICATOR_PROCESSES", "2")
    from dsql_replicator.health import _probe_liveness

    r = _probe_liveness(None)  # type: ignore[arg-type]
    assert not r.ok
    assert "p1" in r.detail
    assert "stale" in r.detail
