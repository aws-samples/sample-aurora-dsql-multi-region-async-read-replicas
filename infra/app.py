"""CDK entry point for the dsql-replicator Fargate deployment.

All deployment parameters are supplied via CDK context (`-c key=value`) or the
AWS environment (`CDK_DEFAULT_ACCOUNT`, `CDK_DEFAULT_REGION`). Example:

    .venv/bin/cdk deploy \\
        -c target_cluster_id=<your-target-cluster-id> \\
        -c target_region=us-west-2

Required context:
    target_cluster_id       — DSQL cluster identifier of the replication target

Optional context (defaults in parens):
    account                 — AWS account for the control-plane region
                              (CDK_DEFAULT_ACCOUNT)
    region                  — control-plane region where the Fargate task runs
                              (CDK_DEFAULT_REGION or us-east-1)
    source_cluster_id       — DSQL cluster id for the replication SOURCE. When
                              set, the replicator verifies CDC is enabled on
                              that cluster at startup and auto-derives the
                              Kinesis stream to consume — you no longer need
                              to pass source_stream_name.
    source_stream_name      — Kinesis stream carrying DSQL CDC. Only required
                              when source_cluster_id is not supplied.
                              (dsql-cdc-stream)
    source_region           — region of the source Kinesis stream (us-east-1)
    target_region           — region of the target DSQL cluster (us-west-2)
    resource_prefix         — name prefix for ECS/log/SNS resources
                              (dsql-replicator)
    alarm_email             — optional email to subscribe to the alarms topic
"""

from __future__ import annotations

import os

import aws_cdk as cdk

from stacks.replicator_stack import DsqlReplicatorStack

app = cdk.App()

account = app.node.try_get_context("account") or os.environ.get("CDK_DEFAULT_ACCOUNT")
region = (
    app.node.try_get_context("region")
    or os.environ.get("CDK_DEFAULT_REGION")
    or "us-east-1"
)

target_cluster_id = app.node.try_get_context("target_cluster_id")
if not target_cluster_id:
    raise SystemExit(
        "Missing required CDK context 'target_cluster_id'. "
        "Pass it with: cdk deploy -c target_cluster_id=<your-target-cluster-id>"
    )

DsqlReplicatorStack(
    app,
    "DsqlReplicatorStack",
    env=cdk.Environment(account=account, region=region),
    source_cluster_id=app.node.try_get_context("source_cluster_id") or None,
    source_stream_name=app.node.try_get_context("source_stream_name") or "dsql-cdc-stream",
    source_region=app.node.try_get_context("source_region") or "us-east-1",
    target_cluster_id=target_cluster_id,
    target_region=app.node.try_get_context("target_region") or "us-west-2",
    resource_prefix=app.node.try_get_context("resource_prefix") or "dsql-replicator",
    alarm_email=app.node.try_get_context("alarm_email") or None,
)

app.synth()
