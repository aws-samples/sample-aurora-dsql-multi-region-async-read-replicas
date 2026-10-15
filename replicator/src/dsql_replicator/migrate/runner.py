"""Top-level migrate orchestrator. Create-or-verify semantics.

Post-M6.4:
- Target DSQL tables (`__replicator_*`) are bootstrapped by `Applier.start()`.
- SNS topics are managed by the CDK stack (co-located with the alarms topic + KMS key).
- `migrate` is CloudWatch dashboard upsert only.

Why keep `migrate` at all? Dashboards aren't a stack artifact in CDK by default (they
leak if you re-deploy under a new stack name), and operators sometimes want to refresh
widgets without a code deploy. `dsql-replicator migrate` is still that one entry point.
"""

from __future__ import annotations

import boto3
import structlog

from dsql_replicator.config import Config

from . import dashboard
from .report import MigrateReport

log = structlog.get_logger("migrate")


def run(cfg: Config) -> MigrateReport:
    log.info("migrate.start", control_region=cfg.control_region)
    report = MigrateReport()
    session = boto3.Session(region_name=cfg.control_region)
    cw_client = session.client("cloudwatch")

    dashboard.ensure_dashboard(cw_client, cfg, report)

    log.info("migrate.done", actions=len(report.actions))
    return report
