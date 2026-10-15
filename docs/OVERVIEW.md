# Aurora DSQL Multi-Region Async Replicator — High-Level Design

A code-free walkthrough of what the system does, how it's shaped, and the choices behind it. 

---

## 1. The problem

An Aurora DSQL cluster in one region publishes committed changes as CDC events to a Kinesis stream you own. The goal is to apply those changes to a second DSQL cluster in another region, asynchronously, so that:

- Every committed source transaction appears on the target as one atomic transaction (readers on the target never see a half-committed source transaction).
- Replay-on-crash never produces duplicates.
- Order within any single row is preserved (if you updated row X twice, the second value wins — never the first).
- Operators have visibility into lag + a clean recovery story when something breaks.
- A single consumer can keep up with real OLTP write rates (tens to hundreds of transactions per second).

The data-plane is **asynchronous** — the target lags the source by seconds under normal load. This is not a synchronous standby; it's a durable, consistent, out-of-region copy.

---

## 2. Why this is non-trivial

Three properties of DSQL's CDC make the straightforward solutions wrong:

1. **The CDC stream is unordered across shards.** Records from one committed transaction can land on different shards and arrive in any order. A naive "apply as you read" loop produces partial transactions on the target.
2. **CDC is net-effect, not an operation log.** Ten updates to the same row in one transaction collapse to a single CDC record with the final value. Deletes carry only the primary key (no pre-image). A replica built on statement replay will diverge.
3. **Cross-region commits are slow.** Each DSQL `BEGIN..COMMIT` from one region to another takes ~300–500 ms. Any design that serializes commits will cap at a few per second — unusable for anything beyond a demo.

Every design choice below exists to make one of these three go away.

---

## 3. The load-bearing primitive

Each CDC record carries two fields that, together, let you buffer a transaction and apply it atomically:

- A **transaction id** that's the same across every record of one committed transaction.
- A **total-records count** that's the same across every record and equals how many records that transaction produced.

With these, the applier can buffer records keyed by transaction id, and release a transaction for commit only when it has seen exactly `total_records` of them. That's the whole correctness story for atomic cross-row apply.

---

## 4. The pipeline, at a glance

```
Source DSQL ──► Kinesis stream ──► Consumer process ──► Target DSQL
                                        │
                                        ├── Reassemble oversized records (chunks)
                                        ├── Group records by transaction id
                                        ├── Buffer by source timestamp (per-shard watermark)
                                        ├── Dispatch concurrent commits (per-key stripes)
                                        └── State plane: idempotency, fence, checkpoints, leases
                                                        (lives inside target DSQL)
```

The consumer is a single long-running process. Internally it has four stages:

1. **Decode** — parse each Kinesis record, join oversized-record fragments, verify checksums.
2. **Group** — buffer records by transaction id until all records of a transaction have arrived.
3. **Order** — hold completed transactions in a priority queue keyed by their source timestamp. Release to the applier only when it's safe (no earlier record can still arrive from any shard).
4. **Apply** — for each released transaction, pick which per-key "lanes" its touched keys belong to, acquire those lanes, run one atomic target-side transaction, release the lanes.

Multiple transactions on disjoint key sets commit concurrently; transactions sharing any key serialize on that key only.

---

## 5. State plane — all in the target cluster

The replicator's own bookkeeping lives inside the target DSQL cluster itself, alongside the data it replicates. Six logical tables:

| Table | Purpose |
|---|---|
| **Applied-stamps** | One row per (transaction id, record id) that was applied. Written inside the same transaction as the row apply. Blocks duplicates on replay with zero application-level logic. |
| **DLQ fence** | When an apply fails permanently, one row records which transaction is blocking forward progress. The applier reads this to decide when to resume. |
| **DLQ payload** | The full stored transaction body for a fenced transaction, so an operator can inspect and retry without replaying Kinesis. |
| **Checkpoints** | Per-shard "last-processed" sequence number, so a restart resumes where the previous process left off. |
| **Shard leases** | Single-writer per shard, with a fencing token and a time-to-live heartbeat. Enables safe failover between processes. |
| **Watermark state** | Per-shard latest timestamp, persisted so a cold restart doesn't start from zero. |

**Why in the target cluster, not DynamoDB?** Because the critical correctness operations — "apply the row, mark it applied, close any fence that covered it, advance the checkpoint" — can all happen in **one** target-side transaction. Nothing is distributed across services; there is no "wrote to DynamoDB but crashed before writing to DSQL" problem to engineer around.

---

## 6. The three design calls that matter

### 6.1 Per-key ordering, not global

Early iterations enforced a global commit order on the target — every transaction had to commit strictly before every later one, keyed by the source's wall-clock timestamp. That's a strong guarantee, but it forces a single commit lane. Under cross-region commit latency (~400 ms), the ceiling is a few transactions per second. Not useful.

The current design enforces order **per key**: two transactions that touch the same row commit in source order; two transactions on disjoint rows commit concurrently. This is the same contract SQL already gives you per row — not a weakening of correctness, a matching of expectations. Industry CDC pipelines (DMS, Debezium Sink) all do this for the same reason.

Measured difference measured: **~6× transactions per second, ~12× rows per second** with the stripe-locked design vs. the serial lane. Hot-key workloads (many writes to one row) are still bounded by cross-region commit latency — physics, not software.

### 6.2 Durable fence instead of fail-fast

When a transaction can't apply (schema mismatch on the target, constraint violation, unknown table), the applier doesn't crash the process and doesn't skip. It opens a **fence** — one row in the target cluster — pinned to the failing transaction's timestamp. The ordering stage then refuses to release any transaction past the fence until an operator resolves it.

The operator's resolution is a single CLI command (retry after fixing the target, or skip with explicit confirmation). Retry re-applies the failed transaction's **stored payload** — no need to go back to Kinesis — and closes the fence inside the same target-side transaction as the row insert. One commit, atomic retry.

### 6.3 Lease-based failover, not stop-the-world

For availability, you can run multiple consumer processes on the same stream. Only one process at a time owns any given shard — enforced by a lease row in the target cluster with a heartbeat and a fencing token. If a process pauses or dies, its lease expires; another process steals it, reads the stored checkpoint, and resumes from the exact sequence number of the last applied record. Any records replayed between the stored checkpoint and the actual last-applied are deduplicated by the applied-stamp table.

The fencing token means a process that pauses and later wakes up cannot accidentally resume as the shard owner — its old token no longer matches, and it shuts the shard worker down cleanly.

---

## 7. Deployment shape

The replicator ships as a single container image, deployed to AWS Fargate behind an ECS service. The surrounding infrastructure:

- A VPC with a NAT gateway for cross-region reach (the target cluster lives in a different region) and VPC endpoints for the AWS services the task talks to in-region (Kinesis, CloudWatch, SNS, ECR).
- A least-privilege IAM task role scoped to only what the replicator needs: read from one Kinesis stream, mint auth tokens for the two DSQL clusters, publish to two SNS topics, write to CloudWatch metrics in one namespace.
- A CloudWatch dashboard with throughput, lag, buffer depth, fence state, and per-shard Kinesis lag.
- Five CloudWatch alarms (apply failures, fence open, ordering buffer near cap, watermark lag, Kinesis records-behind-latest), publishing to a dedicated alarm topic.
- A customer-managed KMS key encrypting both SNS topics at rest.

An operator deploys the whole thing with one command. The runbook covers rollback, log tailing, and the handful of recoveries an on-call engineer might run.

---

## 8. Edge cases the design handles

**Oversized records.** DSQL splits records larger than ~9 MB into a main record plus fragments sharing a chunk id. The decode stage buffers fragments, assembles them in order when the main lands, and verifies a checksum before passing the record on. Orphan fragments and incomplete mains both time out and are logged.

**Out-of-order delivery within a transaction.** The grouping stage buffers by transaction id without any assumption about record order; a transaction only emits when the count matches the declared total.

**Late duplicates after a transaction has emitted.** The grouping stage remembers a bounded number of recently-emitted transaction ids and silently drops stragglers. The applied-stamp table in the target catches anything that slips through.

**Idle shards.** If one shard goes quiet, the pipeline would freeze (its watermark never advances). Idle shards that are confirmed caught-up by Kinesis itself get a synthetic advance after a configurable timeout, with a clock-skew margin so the pipeline resumes correctly.

**Backpressure.** The ordering buffer has a hard cap. When the applier can't keep up, incoming decode slows, which stops advancing the Kinesis iterator — turning Kinesis's 24-hour retention into the real overflow buffer.

**Process crash mid-apply.** The target-side transaction either commits everything (rows + applied-stamp) or nothing. On restart, the ordering stage replays any pending records, and the applied-stamp table deduplicates.

**Permanent apply failure.** DLQ fence (§6.2). Operator inspects, retries, or skips.

**Task crash under write pressure.** Lease failover (§6.3). New task resumes from checkpoint, dedupes any replay.

**Schema drift (DDL on source without a matching change on target).** The applier raises an error on the first affected record, which opens a fence. Operator applies the DDL on target, then runs `dlq retry` — the fence closes atomically with the first retry.

**Cross-region credential expiry.** Short-lived IAM tokens are cached and refreshed on a timer below their TTL; retry paths refresh on demand.

**Replay from Kinesis after an extended operator pause.** Kinesis retention is 24 hours by default, extendable to 7 days. The pipeline's checkpoints survive restarts; a resumed process reads from where it stopped. Beyond retention, the operator loses in-flight records — the design surfaces this via a CloudWatch metric so an alarm can fire well before the data expires.

---

## 9. What's NOT covered

Intentionally out of scope, documented so there are no surprises:

- **DDL replication.** New or altered tables must be applied to both clusters by the operator. The replicator refuses to invent target schema.
- **Cross-transaction atomicity in application semantics.** If your application depends on "transfer money across two accounts" being atomic from the replica's perspective, per-key ordering doesn't preserve that. SQL already doesn't guarantee cross-row atomicity across asynchronously-replicated systems; CDC systems universally match that behavior.
- **Hot-key workloads above a few writes per second per unique row.** Cross-region commit latency is a hard physical floor at the per-key level. No parallelism fixes this.
- **Horizontal scale beyond one consumer process.** The lease model supports multiple processes for availability (failover), but running more than one at the same time currently requires accepting weakened cross-shard ordering guarantees that haven't been designed in yet. Right now, one process at a time.
- **Multi-target fan-out from one process.** One source stream can feed one target cluster per consumer process. Running N targets means running N consumer processes.
- **Admin HTTPS API.** All operator actions are CLI-driven. If you want a web console, you build one on top.
- **Automatic KMS enablement on the Kinesis source stream.** The replicator's own KMS key is managed; attaching it to the pre-existing Kinesis stream is a one-command operator step documented in the runbook, because changing stream encryption affects every producer and consumer of the stream, not just this replicator.

---

## 10. Operational properties

**What the on-call sees.** A dashboard with five single-value tiles up top (fence age, fence count, buffer depth, watermark lag, apply errors), four time-series panels below (throughput, errors + fence lifecycle, lag + backlog, lease activity), and a per-shard Kinesis lag chart.

**Five alarms, feeding one topic:**
- Any apply failure in the last 5 minutes.
- Any open fence (replication is halted; operator action required).
- Ordering buffer over 80% of its cap.
- Watermark more than 10 seconds behind wall clock.
- Any shard more than 60 seconds behind Kinesis latest.

**What the on-call does.** One of three things, in order of likelihood: (a) read the dashboard and wait for a transient to clear, (b) inspect and retry a fenced transaction via the CLI after fixing a schema or constraint issue on the target, (c) scale the service to zero as an emergency full-stop. The runbook has decision trees for each alarm.

**What the on-call does NOT need to do.** Touch DynamoDB (there is none). SSH anywhere (ECS Exec is enabled but rarely needed). Replay Kinesis manually (the stored payload in the fence means retry is a pure target-side operation).

---

## 11. One-sentence summary of each design choice

- Atomic cross-row apply works because every CDC record carries the number of records in its transaction.
- Exactly-once replay works because the applied-stamp lives in the same target transaction as the row.
- Throughput is unlocked because transactions on disjoint keys commit on independent lanes.
- Operator recovery is clean because the stored payload for a failed transaction lives next to the fence that halted it.
- Failover is clean because the shard lease lives next to the data its owner is writing.
- All of the above is simple because the state plane is one place, not three.
