# Deploying the replicator — end-to-end

This is the single page an operator reads to go from clone to replicating.
It chains every step in order and links out to the sub-READMEs for detail —
`infra/README.md`, `loadtest/README.md`, and `replicator/README.md` — so you
do not have to assemble the flow from three places yourself.

Reading time: ~10 min. Hands-on time: ~20 min for a cold deploy on a
pre-existing pair of DSQL clusters.

---

## 1. Prereqs

- An AWS account with permission to create the resources in
  [`infra/README.md`](../infra/README.md) → *Stack contents* (VPC, Fargate,
  CloudWatch, SNS, KMS, IAM).
- **Docker Desktop** running locally. CDK builds the replicator image from
  `replicator/Dockerfile` as part of `cdk deploy`.
- **Python 3.12** on your machine (for the CDK app and the `migrate` CLI).
- **Two Aurora DSQL clusters already created** — one source, one target.
  They may be in different regions. If you do not have them yet, create
  them in the DSQL console or with `aws dsql create-cluster` first.
- The `aws` CLI configured with credentials that can reach both regions
  and call DSQL + Kinesis.

If you want to run the demo without a real source cluster, see
*Mode B* in step 5 — you can skip steps 3–4 entirely and drive the
pipeline with the synthetic publisher under `loadtest/`.

---

## 2. Clone + install

```bash
git clone https://github.com/aws-samples/sample-aurora-dsql-multi-region-async-read-replicas.git
cd sample-aurora-dsql-multi-region-async-read-replicas

# Replicator package (needed for `migrate` and local tests).
cd replicator/
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
cd ..

# CDK app.
cd infra/
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
cd ..
```

---

## 3. Enable DSQL CDC on the source cluster (one-time, external)

**The replicator does not enable CDC for you — it only verifies that CDC is
already on.** This is deliberate: enabling CDC emits every committed
transaction to a Kinesis stream, which is a durable, chargeable change to
your source cluster. The project assumes you turn it on consciously.

To enable CDC, attach a Kinesis stream to the source cluster:

```bash
# 1. Create a Kinesis data stream in the source region.
aws kinesis create-stream \
  --stream-name dsql-cdc-stream \
  --shard-count 2 \
  --region <source-region>

# 2. Create an IAM role DSQL can assume to write to the stream.
#    Trust policy: dsql.amazonaws.com with sts:AssumeRole.
#    Permissions: kinesis:PutRecord(s) on the stream ARN.
#    (See AWS DSQL docs for the exact JSON — the one-time cost is 5 min of
#    IAM console clicking.)

# 3. Attach the stream to the DSQL cluster.
aws dsql create-stream \
  --cluster-identifier <source-cluster-id> \
  --region <source-region> \
  --target-definition kinesis='{streamArn=arn:aws:kinesis:<source-region>:<account>:stream/dsql-cdc-stream,roleArn=arn:aws:iam::<account>:role/dsql-cdc-writer}' \
  --ordering ORDERED_PER_SOURCE \
  --format JSON
```

Verify it reached `ACTIVE`:

```bash
aws dsql list-streams --cluster-identifier <source-cluster-id> --region <source-region>
```

Full reference: see the **Aurora DSQL CDC** section of the official DSQL
docs. If this step is skipped, Mode A's preflight will tell you
"`CDC is not enabled on DSQL cluster '<...>'`" and exit before starting.

---

## 4. CDK bootstrap + state-plane migrate

**CDK bootstrap** (one-time per account+region):

```bash
cd infra/
AWS_REGION=<control-region> .venv/bin/cdk bootstrap
```

**State-plane migrate** (idempotent — bootstraps the state tables the
replicator keeps inside the target DSQL cluster, and upserts the
CloudWatch dashboard):

```bash
cd ../replicator/
SOURCE_STREAMS=dsql-cdc-stream SOURCE_REGION=<source-region> \
  TARGET_CLUSTER_ID=<target-cluster-id> \
  TARGET_REGION=<target-region> \
  .venv/bin/dsql-replicator migrate
```

Expect a JSON report confirming the state tables and dashboard.

---

## 5. Deploy — pick a mode

Both modes deploy the same Fargate service; they differ only in how the
operator names the **source**.

### Mode A — You know the source cluster id (production path)

Preflight calls `dsql:ListStreams` + `dsql:GetStream` on the source cluster
at startup, picks the ACTIVE stream, and reads the Kinesis ARN out of its
CDC configuration. You never type a stream name.

```bash
cd ../infra/
AWS_REGION=<control-region> .venv/bin/cdk deploy \
  -c source_cluster_id=<SOURCE_CLUSTER_ID> \
  -c source_region=<source-region> \
  -c target_cluster_id=<TARGET_CLUSTER_ID> \
  -c target_region=<target-region>
```

If no ACTIVE stream is attached, the task fails fast with a clear
`CDC is not enabled` error instead of silently consuming nothing.

### Mode B — You know the Kinesis stream directly (demo / shared-stream path)

Use this when CDC was wired up outside this project and you already have
the stream name — e.g. a shared ingest stream, or a demo scenario where
`loadtest/synthetic_publisher.py` writes directly to Kinesis.

```bash
AWS_REGION=<control-region> .venv/bin/cdk deploy \
  -c source_stream_name=<KINESIS_STREAM_NAME> \
  -c source_region=<source-region> \
  -c target_cluster_id=<TARGET_CLUSTER_ID> \
  -c target_region=<target-region>
```

`source_stream_name` defaults to `dsql-cdc-stream`, so the minimal command
collapses to just the target context.

Watch the Fargate task come up:

```bash
aws logs tail /aws/ecs/dsql-replicator --follow --region <control-region>
```

The first useful log line to look for is
`preflight: SOURCE_CLUSTER_ID=... → source_streams=['...']` (Mode A) or
the pipeline's `consumer.pipeline started` (both modes).

---

## 6. Generate traffic

See [`loadtest/README.md`](../loadtest/README.md) for the three traffic
patterns. Short decision tree:

| You want to… | Use |
|---|---|
| Drive the whole pipeline from a real DSQL source | `loadtest/generate_load.py` — writes to source DSQL at a steady TPS |
| Load-test the replicator without a source DB (**Mode B** demo) | `loadtest/synthetic_publisher.py` — pumps envelopes straight into Kinesis |
| Test the Postgres-origin path (Aurora PG → DMS → Kinesis) | `loadtest/pg_tps.py` and the pg_* scripts |

Example — synthetic publisher at 50 TPS for 30 min (Mode B demo):

```bash
cd ../loadtest/
TARGET_CLUSTER_ID=<target-cluster-id> TARGET_REGION=<target-region> \
  python _setup_demo_schema.py

python synthetic_publisher.py \
  --stream-name <kinesis-stream> --region <source-region> \
  --target-tps 50 --ramp-start-tps 5 --ramp-s 300 --hold-s 1800
```

---

## 7. Verify replication

**CloudWatch dashboard** (upserted by `migrate` in step 4):

Open the AWS console → CloudWatch → Dashboards → `dsql-replicator`. The
important panels are *Apply TPS*, *End-to-end lag*, *Open fence*, *Ordering
buffer depth*, and *Kinesis records-behind-latest*. Lag should land in the
low seconds under steady state; apply TPS should match the source
production rate.

**Target-side spot check** — connect to the target DSQL cluster and
confirm rows are arriving:

```bash
# IAM auth token for psql:
PGPASSWORD="$(aws dsql generate-db-connect-admin-auth-token \
  --hostname <target-cluster-id>.dsql.<target-region>.on.aws \
  --region <target-region>)"
psql "host=<target-cluster-id>.dsql.<target-region>.on.aws \
      port=5432 dbname=postgres user=admin sslmode=require" \
  -c "SELECT count(*) FROM loadtest_events;"
```

Row count should climb at approximately the source TPS, modulo apply
batching.

For the full on-call view (alarm decision trees, state-plane recipes,
rollback), see [`docs/RUNBOOK.md`](./RUNBOOK.md).

---

## 8. Teardown

```bash
cd ../infra/
AWS_REGION=<control-region> .venv/bin/cdk destroy
```

The CDK-managed asset ECR repo retains images across destroys; delete it
explicitly if you want zero residual spend:

```bash
aws ecr delete-repository \
  --repository-name cdk-hnb659fds-container-assets-<account>-<region> \
  --force --region <control-region>
```

The replicator's state-plane tables live inside the **target DSQL cluster**
and are not touched by `cdk destroy` — drop them manually if you want to
reset state:

```sql
DROP TABLE IF EXISTS
  __replicator_fence,
  __replicator_dlq_payloads,
  __replicator_checkpoints,
  __replicator_shard_leases,
  __replicator_lww_stamps,
  __replicator_watermarks;
```

Likewise, the DSQL CDC stream on the source cluster is **not** disabled by
teardown — if you want to turn CDC off, run
`aws dsql delete-stream --cluster-identifier <source-cluster-id> --stream-identifier <id>`
followed by `aws kinesis delete-stream --stream-name <name>`.

---

## Troubleshooting

| Symptom | Likely cause | Next step |
|---|---|---|
| Preflight: `CDC is not enabled on DSQL cluster '...'` | No ACTIVE stream attached | Step 3 — attach a Kinesis stream to the source cluster |
| Preflight: `SOURCE_STREAMS=[...] disagrees with the streams attached to SOURCE_CLUSTER_ID` | Explicit SOURCE_STREAMS conflicts with the derived set | Drop `-c source_stream_name=…` so Mode A auto-derives, or correct it to match |
| Fargate task flapping, `healthz` failing | Target DSQL unreachable, or state tables missing | Re-run `dsql-replicator migrate`; check VPC endpoints in the control region |
| Lag growing unbounded | Source TPS exceeds applier shape | See `docs/DESIGN.md` → stripe-contention and APPLIER_WORKERS/STRIPES rule of thumb |
| Open-fence alarm firing | Apply failure → DLQ-fence opened | `docs/RUNBOOK.md` → *Open fence* decision tree |

For deeper issues, start with the on-call guide:
[`docs/RUNBOOK.md`](./RUNBOOK.md).
