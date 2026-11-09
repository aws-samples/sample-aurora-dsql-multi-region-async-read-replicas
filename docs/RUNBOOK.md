# dsql-replicator — Operations Runbook

**Pager:** subscribe operators to the stack's alarms SNS topic (CDK output
`AlarmsTopicArn`).

---

## 1. What this replicator does and does NOT do

- **Reads** DSQL CDC records from the configured Kinesis stream (default
  `dsql-cdc-stream`) in the source region.
- **Reassembles** chunked records (>1 MB rows) via a process-global
  FragmentReassembler. Full records pass straight through.
- **Writes** to the configured target DSQL cluster via a stripe-locked
  parallel applier that performs **row-level last-writer-wins** against
  `__replicator_lww_stamps(schema, table, pk, applied_ts_ns)`.
- Guarantees **per-key eventual convergence**: for any primary key, the
  replica eventually reaches the newest source `ts_ns`. Older commits
  that arrive late are rejected by the LWW stamp.
- Does **NOT** preserve **cross-row transactional** invariants. Two source
  transactions touching different rows can be observed partially applied
  on the replica — a state that never existed at the source. See
  `README.md` → "Row-level eventual consistency" and `docs/DESIGN.md` §1a.
- Does **NOT** replicate DDL. New tables must be CREATE'd on both source
  and target before CDC records touch them.
- Does **NOT** currently scale beyond `desiredCount=1`. Multi-task scaling
  needs horizontal shard partitioning (future work).

---

## 2. First thing to do on page

1. **Open the CloudWatch dashboard** `dsql-replicator-<TARGET_CLUSTER_ID>` (us-east-1).
2. **Check the single-value row** at the top:
   - **Apply queue depth** — if growing without bound, apply workers can't keep up; §4.1.
   - **Replication lag (ms)** — if > 10s, the newest applied source commit is stale against wall clock; §4.2.
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

Service `desiredCount=1`, deployment circuit breaker ON with rollback — if
the new task fails healthchecks, ECS rolls back the task definition
automatically.

### Tail logs
```bash
aws logs tail /aws/ecs/dsql-replicator --follow --region us-east-1
aws logs tail /aws/ecs/dsql-replicator --since 10m --region us-east-1 \
  | grep -E "apply.retryable|apply.unexpected|apply.structural"
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
SELECT * FROM __replicator_lww_stamps ORDER BY applied_at DESC LIMIT 20;
SELECT * FROM __replicator_shard_leases;
SELECT * FROM __replicator_checkpoints;
```

---

## 4. Alarms — decision trees

Three alarms fire. Two track end-to-end health; one tracks Kinesis itself.

### 4.1 `dsql-replicator-ApplyQueueNearCap` — apply queue > 80k

The apply workers can't keep up with the shard workers. Short-term, scale
down the source rate (if you own it) and let the queue drain. The writer
autoscaler should already be growing the connection pool toward
`APPLIER_WORKERS_MAX` — confirm it has by searching the logs for
`autoscaler.scaled_up`.

Long-term:
- Confirm your source rate matches the design ceiling (~700 TPS sustained
  at the current 8 vCPU / 16 GB, 128 workers, 8192 stripes shape).
- Hot keys cap at ~3 updates/sec per unique PK — physics, not software.
- Multi-task scaling or multi-shard Kinesis would help; both are future
  work.

### 4.2 `dsql-replicator-ReplicationLag` — > 10s behind source

The newest applied source `ts_ns` is more than 10 seconds stale against
wall clock. Causes, in rough order of frequency:

- **Apply workers behind**: queue depth also high → §4.1.
- **Persistent apply failure on a hot key**: see "Persistent apply failure"
  below.
- **Kinesis lag upstream**: §4.3 should fire concurrently.
- **Idle shard with stuck iterator**: check `shard.get_records.error` in
  logs; may indicate a transient Kinesis issue. Restart the task if a
  single shard is stuck for > 5 minutes.

### 4.3 `dsql-replicator-KinesisMillisBehindLatest` — any shard > 60s behind

Consumer is lagging Kinesis itself, not just the applier. Backpressure
(queue full) or source rate > applier rate. Same remediation as §4.1.

---

## 5. Persistent apply failure — what to do now (no fence)

The 2026-11 simplification pass removed the DLQ fence. The applier now
retries transient failures **forever** with exponential backoff, so a
persistent target-side defect does **not** halt the pipeline — it blocks
only its own stripe locks, while other keys keep flowing.

Signals of a persistent apply failure:
- Growing `ApplyQueueDepth` + growing `ReplicationLag`.
- Log stream shows the same `apply.retryable` or `apply.unexpected_retrying`
  entries repeating for the same `size=N` batch.
- A single stripe appears over and over in `apply.ok` traces for OTHER
  batches, suggesting a hot key is wedged on one connection.

Response:
1. **Identify the failure.** Grep logs for `apply.retryable`,
   `apply.unexpected_retrying`, or `apply.structural_failure_dropping`.
   The `error_class` field names the DSQL error.
2. **Fix the target-side problem.** Common cases: missing column
   (`ALTER TABLE ... ADD COLUMN ...`), CHECK/FK violation, column type
   change, dropped table.
3. **Wait.** The next retry on the stuck record will succeed and the
   pipeline resumes automatically. No CLI step is needed.
4. **If you need to drop a specific record** (e.g. known-bad test data
   bleeding onto a prod stream), the only option is to connect to target
   DSQL and INSERT a `__replicator_lww_stamps` row at a higher
   `applied_ts_ns` than the record — the LWW gate then skips it. Reserve
   this for incident response; audit every use.

`apply.structural_failure_dropping` is distinct: it fires when the record
cannot possibly succeed (schema missing entirely, malformed record). The
pipeline advances past it so other keys keep flowing — the row is lost
for that key until the next source write to the same primary key, which
LWW will apply. Alarm on the log pattern if you want paging on this
case.

---

## 6. Lease recovery (planned task restart)

Fargate service `desiredCount=1`. ECS auto-respawns on task failure.

- **Manual restart:** `aws ecs update-service --cluster dsql-replicator-cluster --service dsql-replicator --force-new-deployment --region us-east-1`. Old task drains, new task steals the lease, resumes from checkpoint.
- **During a crash:** the heartbeat on `__replicator_shard_leases` expires in `LEASE_TTL_SECONDS` (default 30). New task can steal after that. `AFTER_SEQUENCE_NUMBER` iterator resumes from checkpoint; the per-key `ts_ns` guard on `__replicator_lww_stamps` skips any replay (stored `ts_ns` ≥ replayed).
- **Fragments in-flight at crash time** (>1 MB rows) are lost from memory. The LWW model recovers them on the next source write that touches the same primary key. Operationally rare.

---

## 7. Resources the stack creates

The CDK stack creates all of the below in the control region. Default names
assume `resource_prefix=dsql-replicator`; override via CDK context.

| Resource | Default name / shape |
|---|---|
| CloudFormation stack | `DsqlReplicatorStack` |
| ECS cluster | `dsql-replicator-cluster` |
| ECS service | `dsql-replicator`, `desiredCount=1` |
| CloudWatch log group | `/aws/ecs/dsql-replicator` |
| SNS alarms topic | `dsql-replicator-alarms` (KMS-encrypted) |
| KMS CMK | customer-managed, rotation enabled, retention policy RETAIN |

The replicator's **state plane** lives in the target DSQL cluster as
three active tables:

- `__replicator_lww_stamps` — per-key LWW stamp (schema, table, pk,
  applied_ts_ns). Core correctness primitive.
- `__replicator_checkpoints` — per-shard Kinesis sequence resume point.
- `__replicator_shard_leases` — single-writer lease per (stream, shard)
  with heartbeat and fencing token.

Nothing in DynamoDB; nothing in the control region's data plane. If
you are upgrading from a pre-simplification deploy, the removed tables
(`__replicator_applied_v2`, `__replicator_dlq_fences`,
`__replicator_dlq_payload`, `__replicator_dlq_ingest`,
`__replicator_late_discards`, `__replicator_watermark_state`) are no
longer written and can be dropped at operator discretion.

---

## 8. Known limits (not bugs)

- **Row-level, not transaction-level, consistency.** Cross-row invariants
  held by a single source transaction can be observed violated on the
  replica. See `README.md` → "Row-level eventual consistency" for the
  coffee / dinner example.
- **Fragments spanning shards can lose a single row on crash** before the
  next source write. The fragment reassembler is process-global and
  holds fragments in memory. LWW recovers on the next write to the key.
- **Infinite apply retry** burns CPU on a persistent target-side defect.
  Alarm on `ReplicationLag` and `ApplyQueueNearCap` catches it.
- **~700 TPS sustained** at the current single-task shape.
- **Hot-key workloads** are capped at ~3 updates/sec per unique PK
  (cross-region commit latency).
- **DDL is not replicated.** Operator must CREATE/ALTER on target before
  source commits touch the new schema.
- **`desiredCount` stuck at 1** until the ordering contract is revisited.
  The multi-process supervisor (`REPLICATOR_PROCESSES`) provides GIL
  relief within one task; it does not scale across tasks.
- **Multi-process shard starvation.** With `REPLICATOR_PROCESSES > 1`
  and shards ≤ child count, the first child to acquire a lease keeps
  it; siblings idle.
- **`APPLIER_STRIPES` must dwarf `APPLIER_WORKERS × avg_stripes_per_batch`.**
  Rule of thumb: `STRIPES ≥ 64 × WORKERS`. CDK default is 8192 for 128
  workers.

---

## 9. Prod cutover — one-time hardening steps

Not auto-run by CDK (would disrupt active producers/consumers). Operator
should run before promoting a prod environment:

### Enable KMS CMK on the source Kinesis stream
The replicator's KMS CMK is `alias/dsql-replicator` (CDK-managed; encrypts
the alarms SNS topic). To extend to Kinesis:

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

Afterwards, grant the DSQL CDC role
(`arn:aws:iam::<ACCOUNT_ID>:role/dsql-cdc-kinesis-role`)
`kms:GenerateDataKey` on the CMK so DSQL can continue publishing. Grant
the replicator task role (already has it via CDK) `kms:Decrypt` for
consuming.

### Subscribe alarms SNS to on-call
Topic exists (`dsql-replicator-alarms`), KMS-encrypted. Subscribe an email
or PagerDuty endpoint via CDK context on deploy:

```bash
.venv/bin/cdk deploy -c alarm_email=oncall@example.com
```

CDK adds an `EmailSubscription`; AWS sends a confirmation email that must
be clicked once.

## 10. Emergency controls

```bash
# Scale service to 0 (full stop — queue drains, task exits cleanly):
aws ecs update-service --cluster dsql-replicator-cluster --service dsql-replicator \
  --desired-count 0 --region us-east-1

# Restore:
aws ecs update-service --cluster dsql-replicator-cluster --service dsql-replicator \
  --desired-count 1 --region us-east-1
```
