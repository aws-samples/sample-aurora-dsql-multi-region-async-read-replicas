"""Health probes used by `dsql-replicator healthz` and the Fargate HEALTHCHECK.

Three probes, each returning (ok: bool, detail: str):

1. Target DSQL — connect + SELECT 1 (also exercises IAM token generation).
2. SNS — resolve the DLQ events topic (verifies KMS-encrypted topic is reachable).
3. Pipeline liveness — reads `/tmp/dsql-replicator-liveness` mtime. The running
   pipeline's metrics loop touches this file every `METRICS_INTERVAL_MS`. A stale
   file (> `LIVENESS_MAX_AGE_SEC`) means the pipeline is wedged even if AWS probes
   succeed. Added M6.4+ after a token-expiry wedge was found during 500 TPS stress.

Each probe is bounded by a short timeout so a hung dependency doesn't wedge the
container healthcheck. Overall exit code: 0 if ALL checks pass, 1 otherwise.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from pathlib import Path

import boto3
import psycopg
from botocore.exceptions import BotoCoreError, ClientError

from dsql_replicator.applier.connection import (
    DsqlEndpoint,
    DsqlTokenCache,
    open_connection,
)
from dsql_replicator.config import Config

LIVENESS_PATH = "/tmp/dsql-replicator-liveness"
LIVENESS_MAX_AGE_SEC = int(os.environ.get("LIVENESS_MAX_AGE_SEC", "60"))


@dataclass(frozen=True)
class ProbeResult:
    name: str
    ok: bool
    detail: str


async def _probe_dsql(cfg: Config) -> ProbeResult:
    endpoint = _target_endpoint(cfg)
    tokens = DsqlTokenCache(endpoint)
    try:
        conn = await asyncio.wait_for(open_connection(endpoint, tokens, autocommit=True), timeout=10)
    except Exception as exc:
        return ProbeResult("dsql", False, f"connect failed: {exc}")
    try:
        async with conn.cursor() as cur:
            await asyncio.wait_for(cur.execute("SELECT 1"), timeout=5)
            row = await cur.fetchone()
        if row is None or row[0] != 1:
            return ProbeResult("dsql", False, "SELECT 1 returned no row")
        return ProbeResult("dsql", True, "ok")
    except (psycopg.Error, TimeoutError) as exc:
        return ProbeResult("dsql", False, f"query failed: {exc}")
    finally:
        await conn.close()


def _probe_sns(cfg: Config) -> ProbeResult:
    client = boto3.client("sns", region_name=cfg.control_region)
    try:
        # create_topic is idempotent and returns the existing ARN unchanged (does not
        # modify KMS attributes). CDK owns topic creation + KMS; this probe just verifies
        # reachability + ARN resolution.
        resp = client.create_topic(Name=cfg.dlq_sns_topic_name)
        client.get_topic_attributes(TopicArn=resp["TopicArn"])
        return ProbeResult("sns", True, f"topic {cfg.dlq_sns_topic_name} ok")
    except (ClientError, BotoCoreError) as exc:
        return ProbeResult("sns", False, f"topic probe failed: {exc}")


def _probe_liveness(_: Config) -> ProbeResult:
    """Verify the running pipeline's metrics loop has touched the heartbeat file recently.

    If the file is missing (e.g. first 10s after container start) we treat it as
    not-yet-ready and return False — Fargate's HEALTHCHECK `start_period` grace
    window absorbs that until the pipeline writes its first heartbeat.
    """
    p = Path(LIVENESS_PATH)
    if not p.exists():
        return ProbeResult("liveness", False, "heartbeat file missing")
    try:
        age = time.time() - p.stat().st_mtime
    except OSError as exc:
        return ProbeResult("liveness", False, f"stat failed: {exc}")
    if age > LIVENESS_MAX_AGE_SEC:
        return ProbeResult("liveness", False, f"heartbeat stale: {age:.1f}s > {LIVENESS_MAX_AGE_SEC}s")
    return ProbeResult("liveness", True, f"heartbeat {age:.1f}s old")


async def run_healthz(cfg: Config) -> list[ProbeResult]:
    """Run all probes concurrently and return the result list."""
    dsql_task = asyncio.create_task(_probe_dsql(cfg))
    sns_task = asyncio.to_thread(_probe_sns, cfg)
    liveness_task = asyncio.to_thread(_probe_liveness, cfg)
    results = await asyncio.gather(dsql_task, sns_task, liveness_task)
    return list(results)


def _target_endpoint(cfg: Config) -> DsqlEndpoint:
    host = f"{cfg.target_cluster_id}.dsql.{cfg.target_region}.on.aws"
    return DsqlEndpoint(
        hostname=host,
        region=cfg.target_region,
        endpoint_url=cfg.target_endpoint_url,
        cluster_identifier=cfg.target_cluster_id,
    )
