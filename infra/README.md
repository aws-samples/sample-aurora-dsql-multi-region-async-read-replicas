# dsql-replicator — Fargate deployment

CDK Python app that deploys the replicator as a single ECS Fargate service in
the control region. The CDK stack owns compute, encryption, alarms, VPC, and
both SNS topics. The CloudWatch dashboard is upserted via
`dsql-replicator migrate` (one-shot). The replicator's own state plane
(idempotency stamps, fences, checkpoints, shard leases) lives in the target
DSQL cluster — nothing in DynamoDB.

## Stack contents

`DsqlReplicatorStack` deploys:

- VPC (2 AZs) with one NAT gateway for cross-region DSQL egress
- Interface endpoints: Kinesis, SNS, CloudWatch Logs, CloudWatch Monitoring,
  ECR API, ECR DKR
- Gateway endpoints: DynamoDB, S3 (ECR layer pulls)
- CDK Docker-image asset (builds `../replicator/Dockerfile`; no manual push)
- CloudWatch log group `/aws/ecs/dsql-replicator`
- IAM task role (least-privilege) + ECS task-exec role
- Fargate task definition and service (`desiredCount=1`, deployment circuit
  breaker on, no public IP)
- Customer-managed KMS CMK + two SNS topics (DLQ events, alarms)
- Five CloudWatch alarms (apply failures, open fence, ordering buffer, lag,
  Kinesis records-behind-latest)

## Prereqs

```bash
# 1. AWS credentials in your environment (any mechanism — profile, SSO, env vars).

# 2. Docker running (CDK will shell out to it to build the image).

# 3. CDK Python venv:
cd infra/
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 4. CDK bootstrap (one-time per account/region):
AWS_REGION=<control-region> .venv/bin/cdk bootstrap

# 5. State plane bootstrap (idempotent; safe to re-run):
cd ../replicator/
SOURCE_STREAMS=dsql-cdc-stream SOURCE_REGION=<source-region> \
  TARGET_CLUSTER_ID=<your-target-cluster-id> \
  TARGET_REGION=<target-region> \
  TARGET_ENDPOINT_URL=https://dsql.<target-region>.api.aws \
  .venv/bin/dsql-replicator migrate
```

## Deploy

```bash
cd infra/
AWS_REGION=<control-region> .venv/bin/cdk diff \
  -c target_cluster_id=<your-target-cluster-id> \
  -c target_region=<target-region> \
  -c target_endpoint_url=https://dsql.<target-region>.api.aws

AWS_REGION=<control-region> .venv/bin/cdk deploy \
  -c target_cluster_id=<your-target-cluster-id> \
  -c target_region=<target-region> \
  -c target_endpoint_url=https://dsql.<target-region>.api.aws
```

Watch logs:

```bash
aws logs tail /aws/ecs/dsql-replicator --follow --region <control-region>
```

## Teardown

```bash
.venv/bin/cdk destroy
# The CDK-managed asset ECR repo retains images. If you want to remove it:
# aws ecr delete-repository --repository-name cdk-hnb659fds-container-assets-... \
#   --force --region <control-region>
```
