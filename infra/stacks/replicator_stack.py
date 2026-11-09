"""Fargate deployment stack for the DSQL replicator.

Co-locates everything into one stack so the whole deployment is reviewable here:

- VPC (2 AZs, 1 NAT) in the control region. NAT exists ONLY to reach cross-region DSQL
  in us-west-2; everything else goes through VPC endpoints.
- VPC endpoints: Kinesis / SNS / CloudWatch Logs / CloudWatch Monitoring / ECR API /
  ECR DKR (interface) + DynamoDB / S3 (gateway).
- ECR repo `dsql-replicator` (retain on stack delete so pushed images survive).
- CloudWatch log group `/aws/ecs/dsql-replicator`.
- Task role with least-privilege policies (Kinesis / DSQL / DDB / SNS / CloudWatch).
- Execution role (ECR pull + CW Logs).
- Task definition (1 vCPU / 2 GB) + Fargate service (desiredCount=1, circuit breaker on).

The replicator's state plane (DDB, SNS topic, dashboard) is NOT managed here;
`dsql-replicator migrate` owns it and must be run first. See infra/README.md.
"""

from __future__ import annotations

from pathlib import Path

import aws_cdk as cdk
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subs
from constructs import Construct


class DsqlReplicatorStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        source_stream_name: str,
        source_region: str,
        target_cluster_id: str,
        target_region: str,
        resource_prefix: str,
        source_cluster_id: str | None = None,
        alarm_email: str | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ---- VPC + endpoints ------------------------------------------------
        vpc = ec2.Vpc(
            self,
            "Vpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        # Gateway endpoints (free). DynamoDB + S3 (ECR pulls image layers from S3).
        vpc.add_gateway_endpoint("DynamoDbEndpoint", service=ec2.GatewayVpcEndpointAwsService.DYNAMODB)
        vpc.add_gateway_endpoint("S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3)

        # Interface endpoints keep traffic to AWS services off the NAT.
        for name, service in (
            ("Kinesis", ec2.InterfaceVpcEndpointAwsService.KINESIS_STREAMS),
            ("Sns", ec2.InterfaceVpcEndpointAwsService.SNS),
            ("Logs", ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS),
            ("Cloudwatch", ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH),
            ("EcrApi", ec2.InterfaceVpcEndpointAwsService.ECR),
            ("EcrDkr", ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER),
        ):
            vpc.add_interface_endpoint(
                f"{name}Endpoint",
                service=service,
                subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
                private_dns_enabled=True,
            )

        # ---- Container image (CDK asset) -----------------------------------
        # CDK builds the image from ../replicator and pushes it into a CDK-managed ECR
        # repo during `cdk deploy`. First deploy = first push; subsequent deploys only
        # re-upload layers that changed.
        docker_context = Path(__file__).resolve().parent.parent.parent / "replicator"
        image_asset = ecr_assets.DockerImageAsset(
            self,
            "Image",
            directory=str(docker_context),
            platform=ecr_assets.Platform.LINUX_AMD64,
        )

        # ---- Log group ------------------------------------------------------
        log_group = logs.LogGroup(
            self,
            "LogGroup",
            log_group_name="/aws/ecs/dsql-replicator",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # ---- Task role: least-privilege IAM --------------------------------
        task_role = iam.Role(
            self,
            "TaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            description="dsql-replicator Fargate task role (least-privilege)",
        )

        # Kinesis consumer on the source stream.
        task_role.add_to_policy(
            iam.PolicyStatement(
                sid="KinesisSourceConsume",
                actions=[
                    "kinesis:DescribeStream",
                    "kinesis:DescribeStreamSummary",
                    "kinesis:GetRecords",
                    "kinesis:GetShardIterator",
                    "kinesis:ListShards",
                    "kinesis:SubscribeToShard",
                ],
                resources=[
                    f"arn:aws:kinesis:{source_region}:{self.account}:stream/{source_stream_name}",
                    f"arn:aws:kinesis:{source_region}:{self.account}:stream/{source_stream_name}/*",
                ],
            )
        )

        # DSQL admin IAM auth tokens for both clusters.
        # dsql:DbConnectAdmin covers admin-token issuance; we scope by cluster ARN
        # where the admin principal is the one authorized to replicate.
        task_role.add_to_policy(
            iam.PolicyStatement(
                sid="DsqlAdminTokens",
                actions=["dsql:DbConnectAdmin"],
                resources=[
                    f"arn:aws:dsql:{source_region}:{self.account}:cluster/*",
                    f"arn:aws:dsql:{target_region}:{self.account}:cluster/*",
                ],
            )
        )

        # DSQL control-plane reads: preflight uses these to verify CDC is enabled
        # on the source cluster and auto-derive the Kinesis stream when the
        # operator passes `source_cluster_id`. Scoped to the source region only.
        task_role.add_to_policy(
            iam.PolicyStatement(
                sid="DsqlSourcePreflight",
                actions=["dsql:ListStreams", "dsql:GetStream", "dsql:GetCluster"],
                resources=[f"arn:aws:dsql:{source_region}:{self.account}:cluster/*"],
            )
        )

        # State plane now lives in target DSQL — no DynamoDB policy. Fence rows,
        # payloads, checkpoints, shard leases, and watermark mirrors are all in
        # target-DSQL tables bootstrapped by `Applier.start()`. Permission to touch
        # them comes from `dsql:DbConnectAdmin` above.

        # ---- KMS CMK ------------------------------------------------------
        # Encrypts the alarms SNS topic. DSQL does its own at-rest encryption
        # with service keys — no CMK work needed there. Kinesis stream
        # encryption is a one-time operator action (see RUNBOOK) because the
        # stream was created outside this stack.
        kms_key = kms.Key(
            self,
            "ReplicatorKey",
            alias="alias/dsql-replicator",
            description="CMK for dsql-replicator alarms SNS topic",
            enable_key_rotation=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
        kms_key.grant_encrypt_decrypt(task_role)

        # CloudWatch metrics (M5 metrics emission).
        task_role.add_to_policy(
            iam.PolicyStatement(
                sid="CwPutMetrics",
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
                conditions={"StringEquals": {"cloudwatch:namespace": "DSQLReplicator"}},
            )
        )

        # ---- ECS cluster + task definition + service -----------------------
        cluster = ecs.Cluster(
            self,
            "Cluster",
            vpc=vpc,
            cluster_name="dsql-replicator-cluster",
            container_insights_v2=ecs.ContainerInsights.ENABLED,
        )

        # M7.0c vertical scaling + multi-process (A+C): 8 vCPU / 16 GB with
        # REPLICATOR_PROCESSES=2 so each child owns its own asyncio loop and the GIL
        # stops being the per-task ceiling. Each child runs 128 workers / 512 stripes,
        # giving 2 x 128 = 256 total concurrent apply lanes against the target
        # (same connection load as the previous single-process shape). Micro-batching
        # (BATCH_MAX_TXNS=16) packs 16 source txns into one cross-region commit.
        task_def = ecs.FargateTaskDefinition(
            self,
            "TaskDef",
            cpu=8192,
            memory_limit_mib=16384,
            task_role=task_role,
        )
        # obtain_execution_role materializes the role lazily; CDK already attaches the
        # managed ECS task-exec policy to it. We only need to grant repo pull.
        exec_role = task_def.obtain_execution_role()
        image_asset.repository.grant_pull(exec_role)

        container = task_def.add_container(
            "Replicator",
            image=ecs.ContainerImage.from_docker_image_asset(image_asset),
            logging=ecs.LogDrivers.aws_logs(stream_prefix="replicator", log_group=log_group),
            environment={
                # When SOURCE_CLUSTER_ID is set, the replicator auto-derives
                # SOURCE_STREAMS from the cluster's CDC config at startup and
                # ignores the SOURCE_STREAMS passed here (preflight verifies
                # the two agree if the operator sets both).
                "SOURCE_STREAMS": source_stream_name,
                "SOURCE_CLUSTER_ID": source_cluster_id or "",
                "SOURCE_REGION": source_region,
                "TARGET_CLUSTER_ID": target_cluster_id,
                "TARGET_REGION": target_region,
                "RESOURCE_PREFIX": resource_prefix,
                "CONTROL_REGION": source_region,
                "LOG_LEVEL": "INFO",
                # M7.0c: 2 child processes, each 128 workers / 512 stripes. Total
                # concurrent apply lanes to target = 256 (same as prior single-proc).
                "REPLICATOR_PROCESSES": "2",
                "APPLIER_WORKERS": "128",
                # Autoscale ceiling. Writer pool grows 128 → 256 when
                # end-to-end lag exceeds 10s for 60s; shrinks back on 2s/5min.
                # Set equal to APPLIER_WORKERS to disable scaling.
                "APPLIER_WORKERS_MAX": "256",
                # STRIPES must dwarf APPLIER_WORKERS_MAX * stripes-per-batch
                # (the ceiling, not the floor), else once the autoscaler grows
                # the pool past 128 workers they queue on asyncio stripe locks
                # and CPU stays ~2%. 16384 = 64 × 256-worker ceiling. See the
                # 2026-10-26 stripe-contention finding in docs/DESIGN.md.
                "APPLIER_STRIPES": "16384",
                # Micro-batching (A): 2 source txns per cross-region commit, flush
                # partial batches after 50 ms. Larger values (16) measured 2026-10-26
                # produced ~50 stripes/batch which near-fully serialized on stripe
                # locks and collapsed applier throughput to ~11 TPS. BATCH=2 keeps
                # stripes/batch at ~7, which plays well with APPLIER_STRIPES=8192.
                "BATCH_MAX_TXNS": "2",
                "BATCH_MAX_WAIT_MS": "50",
                # Re-list Kinesis shards every 5 min so a mid-run reshard
                # (e.g. 10 → 12 shards) is picked up without rolling the task.
                # Set to "0" to disable periodic re-discovery.
                "SHARD_DISCOVERY_INTERVAL_MS": "300000",
            },
            health_check=ecs.HealthCheck(
                command=["CMD-SHELL", "dsql-replicator healthz || exit 1"],
                interval=cdk.Duration.seconds(30),
                timeout=cdk.Duration.seconds(15),
                retries=3,
                start_period=cdk.Duration.seconds(60),
            ),
            command=["consume"],
        )
        _ = container  # keep lint happy; container is attached to task_def by add_container

        service = ecs.FargateService(
            self,
            "Service",
            cluster=cluster,
            task_definition=task_def,
            desired_count=1,
            assign_public_ip=False,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            circuit_breaker=ecs.DeploymentCircuitBreaker(rollback=True),
            min_healthy_percent=0,  # single-task service: allow the old one to go away.
            max_healthy_percent=200,
            enable_execute_command=True,
            service_name="dsql-replicator",
        )

        # ---- Alarm SNS topic (operator paging / on-call) -------------------
        alarm_topic = sns.Topic(
            self,
            "AlarmsTopic",
            topic_name="dsql-replicator-alarms",
            display_name="DSQL replicator alarms",
            master_key=kms_key,
        )
        if alarm_email:
            alarm_topic.add_subscription(sns_subs.EmailSubscription(alarm_email))
        alarm_action = cw_actions.SnsAction(alarm_topic)

        def _metric(
            name: str,
            *,
            unit: cw.Unit = cw.Unit.COUNT,
            stat: str = "Sum",
            dims: dict[str, str] | None = None,
            period: cdk.Duration = cdk.Duration.minutes(1),
        ) -> cw.Metric:
            return cw.Metric(
                namespace="DSQLReplicator",
                metric_name=name,
                statistic=stat,
                period=period,
                unit=unit,
                dimensions_map=(dims or {"TargetCluster": target_cluster_id, "Stream": source_stream_name}),
            )

        def _alarm(
            name: str,
            metric: cw.Metric,
            threshold: float,
            *,
            comparison: cw.ComparisonOperator = cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            evaluation_periods: int = 5,
            datapoints_to_alarm: int | None = None,
            treat_missing_data: cw.TreatMissingData = cw.TreatMissingData.NOT_BREACHING,
            description: str = "",
        ) -> cw.Alarm:
            a = cw.Alarm(
                self,
                name,
                alarm_name=f"dsql-replicator-{name}",
                metric=metric,
                threshold=threshold,
                comparison_operator=comparison,
                evaluation_periods=evaluation_periods,
                datapoints_to_alarm=datapoints_to_alarm or evaluation_periods,
                treat_missing_data=treat_missing_data,
                alarm_description=description,
            )
            a.add_alarm_action(alarm_action)
            a.add_ok_action(alarm_action)
            return a

        # Post-2026-11 simplification: no fence / no ordering buffer / no
        # synthetic watermark, so the alarms that gated on those metrics are
        # gone. Row-level LWW is still visible operationally through
        # replication lag, apply queue depth, and raw Kinesis lag.
        _alarm(
            "ApplyQueueNearCap",
            _metric("ApplyQueueDepth", stat="Maximum"),
            threshold=80_000,
            evaluation_periods=5,
            description=(
                "Apply queue >80k entries (cap 100k). Apply workers can't keep up with "
                "the shard workers; backpressure imminent."
            ),
        )
        _alarm(
            "ReplicationLag",
            _metric("ReplicationLagMs", unit=cw.Unit.MILLISECONDS, stat="Average"),
            threshold=10_000,
            evaluation_periods=5,
            description=(
                "Newest applied source commit is >10s stale against wall clock. "
                "Check the apply queue and target DSQL health."
            ),
        )
        _alarm(
            "KinesisMillisBehindLatest",
            _metric("KinesisMillisBehindLatest", unit=cw.Unit.MILLISECONDS, stat="Maximum"),
            threshold=60_000,
            evaluation_periods=5,
            description="Any shard >60s behind Kinesis latest. Consumer is lagging the source stream.",
        )

        # ---- Outputs --------------------------------------------------------
        cdk.CfnOutput(self, "ImageUri", value=image_asset.image_uri)
        cdk.CfnOutput(self, "ServiceName", value=service.service_name)
        cdk.CfnOutput(self, "LogGroupName", value=log_group.log_group_name)
        cdk.CfnOutput(self, "ClusterName", value=cluster.cluster_name)
        cdk.CfnOutput(self, "AlarmsTopicArn", value=alarm_topic.topic_arn)
        cdk.CfnOutput(self, "KmsKeyArn", value=kms_key.key_arn)
