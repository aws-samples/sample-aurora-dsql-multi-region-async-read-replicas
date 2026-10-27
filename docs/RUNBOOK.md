# dsql-replicator — Operations Runbook

**Pager:** subscribe operators to the stack's alarms SNS topic (CDK output
`AlarmsTopicArn`).

---

## 1. What this replicator does and does NOT do

- **Reads** DSQL CDC records from the configured Kinesis stream (default
  `dsql-cdc-stream`) in the source region.
- **Reassembles** chunked records, groups by `txId`, orders per-key by `source.ts_ns`.
- **Writes** to the configured target DSQL cluster with per-key parallel apply.
- Preserves **per-key ordering** (not global). Two txns on disjoint keys may commit out of global `ts_ns` order; txns on the same key always commit in source order.
- Idempotent on replay: the `__replicator_applied_v2(schema, table, pk, applied_ts_ns)` stamp table enforces per-key last-writer-wins by source `ts_ns`.
- Does **NOT** replicate DDL. New tables must be CREATE'd on both source and target before CDC records touch them.
- Does **NOT** currently scale beyond `desiredCount=1`. Multi-task scaling needs a future ordering-contract revisit.

---

## 2. First thing to do on page

1. **Open the CloudWatch dashboard** `dsql-replicator-<TARGET_CLUSTER_ID>` (us-east-1).
2. **Check the single-value row** at the top:
   - **Open fence age (s)** — if > 0, a DLQ fence is OPEN; forward progress halted. Go to §4.1.
   - **Open fence count** — same signal.
   - **Ordering buffer depth** — if growing without an alarm, applier is lagging; §4.3.
   - **Watermark lag (ms)** — if > 10s, consumer is behind source; §4.4.
3. **Which alarm fired?** `aws cloudwatch describe-alarms --state-value ALARM --region us-east-1 --query 'MetricAlarms[*].AlarmName'`. Then jump to §4.

---

## 3. Deploy / rollback / log tail

### Deploy a new image
```bash
cd infra/
.venv/bin/cdk diff           # dry run
.venv/bin/cdk deploy         # 3–8 min; service rolls new task
```

### Roll back
```bash
# Fastest: redeploy from the previous git commit.
git checkout <previous-good-sha>
cd infra/
.venv/bin/cdk deploy
```

Service `desiredCount=1`, deployment circuit breaker ON with rollback — if the new task fails healthchecks, ECS rolls back the task definition automatically.

### Tail logs
```bash
aws logs tail /aws/ecs/dsql-replicator --follow --region us-east-1
aws logs tail /aws/ecs/dsql-replicator --since 10m --region us-east-1 | grep -E "apply.failed|fence|error"
```

### Inspect state tables (target DSQL)
```bash
# Get a psql token:
export TGT=<TARGET_CLUSTER_ID>.dsql.us-west-2.on.aws
PGPASSWORD=$(env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
  aws dsql generate-db-connect-admin-auth-token \
    --hostname "$TGT" --region us-west-2 \
    --endpoint-url https://dsql.us-west-2.api.aws) \
psql "host=$TGT port=5432 dbname=postgres user=admin sslmode=require"

# Then inside psql:
SELECT * FROM __replicator_dlq_fences WHERE status='OPEN';
SELECT * FROM __replicator_shard_leases;
SELECT * FROM __replicator_checkpoints;
```

---

## 4. Alarms — decision trees

### 4.1 `dsql-replicator-OpenFence` — a DLQ fence is open

Forward progress is halted. Operator action required.

```bash
# Who's fenced?
.venv/bin/dsql-replicator ops dlq list

# Inspect the fence + stored payload
.venv/bin/dsql-replicator ops dlq inspect <tx_id>
```

Decide:

- **Fix the underlying issue + retry.** E.g. missing column on target: `ALTER TABLE ... ADD COLUMN ...` on target. Then:
  ```bash
  .venv/bin/dsql-replicator ops dlq retry <tx_id>
  ```
  Retry re-applies rows and closes fence atomically (same DSQL txn).

- **Skip (data loss!) + continue.** Only if the txn is known-bad (e.g. test data on prod stream). Requires `--confirm`. In prod (`REQUIRE_SKIP_SECOND_APPROVAL=true`), also requires `--approved-by <second-op>`:
  ```bash
  .venv/bin/dsql-replicator ops dlq skip <tx_id> --confirm --approved-by <second-op>
  ```

- **Edit payload + retry.** If the schema shifted and the stored payload needs tweaking:
  ```bash
  .venv/bin/dsql-replicator ops dlq edit-and-retry <tx_id>
  # opens $EDITOR on the payload JSON, saves it, retries.
  ```

**After close:** consumer's `_fence_poller` picks up the CLOSED state within `FENCE_POLL_INTERVAL_MS` (default 5000 ms) and resumes applying. Watch the dashboard — `OrderingBufferDepth` should drain.

### 4.2 `dsql-replicator-ApplyErrors` — apply failures observed

Count of permanent apply failures in the last 5 min. If this is firing, there's likely a fence open (§4.1). If not, the applier is throwing errors but recovering — check logs for the `apply.failed` and `apply.permanent` entries:

```bash
aws logs tail /aws/ecs/dsql-replicator --since 10m --region us-east-1 | grep -E "apply.failed|apply.permanent"
```

Common causes: schema mismatch (missing column, type change), constraint violation, serialization conflict (persistent — rare).

### 4.3 `dsql-replicator-OrderingBufferNearCap` — buffer > 80k

The applier can't keep up with source rate. Short-term:

```bash
# Pause the source side writes if you own them, or:
.venv/bin/dsql-replicator ops pause   # halts replicator; backlog drains the Kinesis stream to its 24h retention
# ... reduce source rate ...
.venv/bin/dsql-replicator ops unpause
```

Long-term:
- Confirm your source rate matches the design ceiling (M6.0: ~8 TPS × ~3 DMLs/txn ≈ 25 DMLs/sec per task).
- Hot keys cap at ~3 updates/sec per unique PK — physics, not software.
- Multi-task scaling or multi-shard Kinesis would help; both are future work.

### 4.4 `dsql-replicator-WatermarkLag` — > 10s behind source

Watermark reflects the oldest unprocessed per-shard `ts_ns`. Rising = the applier isn't releasing from the ordering buffer. Causes:

- Open fence (§4.1) — expected.
- Slow applier (§4.3).
- Idle shard with `MillisBehindLatest > 0` — stuck Kinesis iterator. Check `shard.get_records.error` in logs; may indicate a transient Kinesis issue.

### 4.5 `dsql-replicator-KinesisMillisBehindLatest` — any shard > 60s behind

Consumer is lagging Kinesis itself, not just the applier. Backpressure (buffer full) + source rate > applier rate. Same remediation as §4.3.

---

## 5. Lease recovery (planned task restart)

Fargate service `desiredCount=1`. ECS auto-respawns on task failure.

- **Manual restart:** `aws ecs update-service --cluster dsql-replicator-cluster --service dsql-replicator --force-new-deployment --region us-east-1`. Old task drains, new task steals the lease, resumes from checkpoint.
- **During a crash:** the heartbeat on `__replicator_shard_leases` expires in `LEASE_TTL_SECONDS` (default 30). New task can steal after that. `AFTER_SEQUENCE_NUMBER` iterator resumes from checkpoint; the per-key `ts_ns` guard on `__replicator_applied_v2` skips any replay (stored `ts_ns` ≥ replayed).

---

## 6. Resources the stack creates

The CDK stack creates all of the below in the control region. Default names
assume `resource_prefix=dsql-replicator`; override via CDK context.

| Resource | Default name / shape |
|---|---|
| CloudFormation stack | `DsqlReplicatorStack` |
| ECS cluster | `dsql-replicator-cluster` |
| ECS service | `dsql-replicator`, `desiredCount=1` |
| CloudWatch log group | `/aws/ecs/dsql-replicator` |
| SNS DLQ events topic | `dsql-replicator-dlq-events` (KMS-encrypted) |
| SNS alarms topic | `dsql-replicator-alarms` (KMS-encrypted) |
| KMS CMK | customer-managed, rotation enabled, retention policy RETAIN |

The replicator's **state plane** lives in the target DSQL cluster as six
active tables: `__replicator_applied_v2` (per-key LWW stamp),
`__replicator_dlq_fences`, `__replicator_dlq_payload`,
`__replicator_checkpoints`, `__replicator_shard_leases`,
`__replicator_watermark_state`. (The old `__replicator_applied(tx_id,
record_id)` table may still exist from pre-2026-10-26 deploys; it is no
longer written and can be dropped at operator discretion.) Nothing in
DynamoDB; nothing in the control region's data plane.

---

## 7. Known limits (not bugs)

- **~700 TPS sustained** at the current single-task shape (8 vCPU / 16 GB,
  128 workers, 8192 stripes, BATCH_MAX_TXNS=2, REPLICATOR_PROCESSES=2).
  Earlier builds (32–256 workers / 64–512 stripes) measured 8–250 TPS.
  Above 700 TPS, watermark lag grows linearly; the next lever is
  horizontal task scale with shard partitioning (open TODO).
- **Hot-key workloads** are capped at ~3 updates/sec per unique PK (cross-region commit latency).
- **DDL is not replicated.** Operator must CREATE/ALTER on target before source commits touch the new schema.
- **Cross-key atomicity** in application semantics (e.g. "transfer money across accounts") is NOT preserved — only per-row (per-key) order. This is standard CDC behavior.
- **`desiredCount` stuck at 1** until the ordering contract is revisited
  (future work). The multi-process supervisor (`REPLICATOR_PROCESSES`)
  provides GIL relief within one task; it does not scale across tasks.
- **Multi-process shard starvation.** With `REPLICATOR_PROCESSES > 1` and
  shards ≤ child count, the first child to acquire a lease keeps it;
  siblings idle. The GIL-relief multi-process is intended to deliver is
  only realized when source shards ≥ children (open TODO #18).
- **`APPLIER_STRIPES` must dwarf `APPLIER_WORKERS × avg_stripes_per_batch`.**
  Rule of thumb: `STRIPES ≥ 64 × WORKERS`. The 2026-10-26 incident was
  128 workers × 512 stripes collapsing to 88 TPS from stripe-lock
  contention; 8192 stripes restored 700 TPS.

---

## 8. Prod cutover — one-time hardening steps

Not auto-run by CDK (would disrupt active producers/consumers). Operator should run before promoting a prod environment:

### Enable KMS CMK on the source Kinesis stream
The replicator's KMS CMK is `alias/dsql-replicator` (CDK-managed, same key encrypts both SNS topics). To extend to Kinesis:

```bash
env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
  aws kinesis start-stream-encryption \
    --stream-name dsql-cdc-stream \
    --encryption-type KMS \
    --key-id alias/dsql-replicator \
    --region us-east-1

# Verify (encryption state is eventual; may take ~30s):
env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
  aws kinesis describe-stream-summary \
    --stream-name dsql-cdc-stream --region us-east-1 \
    --query 'StreamDescriptionSummary.{Encryption:EncryptionType,KeyId:KeyId}'
```

Afterwards, grant the DSQL CDC role (`arn:aws:iam::<ACCOUNT_ID>:role/dsql-cdc-kinesis-role`) `kms:GenerateDataKey` on the CMK so DSQL can continue publishing. Grant the replicator task role (already has it via CDK) `kms:Decrypt` for consuming.

### Promote `REQUIRE_SKIP_SECOND_APPROVAL=true` in prod
Gamma leaves it `false`. Prod CDK stack sets `REQUIRE_SKIP_SECOND_APPROVAL=true` in the container env; `ops dlq skip` then requires `--approved-by <id>` with id different from invoker.

### Subscribe alarms SNS to on-call
Topic exists (`dsql-replicator-alarms`), KMS-encrypted. Subscribe an email or PagerDuty endpoint via CDK context on deploy:

```bash
.venv/bin/cdk deploy -c alarm_email=oncall@example.com
```

CDK adds an `EmailSubscription`; AWS sends a confirmation email that must be clicked once.

## 9. Emergency controls

```bash
# Pause everything (fence at ts_ns=0; halts all apply):
.venv/bin/dsql-replicator ops pause

# Resume:
.venv/bin/dsql-replicator ops unpause

# Scale service to 0 (full stop):
aws ecs update-service --cluster dsql-replicator-cluster --service dsql-replicator \
  --desired-count 0 --region us-east-1

# Restore:
aws ecs update-service --cluster dsql-replicator-cluster --service dsql-replicator \
  --desired-count 1 --region us-east-1
```
