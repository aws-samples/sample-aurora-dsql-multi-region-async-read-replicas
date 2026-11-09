# Aurora DSQL Multi-Region Async Replicator — High-Level Design

A code-free walkthrough of what the system does, how it's shaped, and the choices behind it.

> **2026-11 simplification pass.** This document describes the current design: **row-level last-writer-wins (LWW)** against a stamp table in the target cluster. The former cross-shard transaction reassembly, ordering buffer with watermark gate, and DLQ fence were removed in Nov 2026 after the DSQL service team confirmed Kinesis cannot preserve DSQL transaction order across shards — so that machinery never actually delivered the transactional guarantee its name implied. See `DESIGN.md §1a` for the current contract and the "Caleb's coffee / dinner" example.

---

## 1. The problem

An Aurora DSQL cluster in one region publishes committed changes as CDC events to a Kinesis stream you own. The goal is to apply those changes to a second DSQL cluster in another region, asynchronously, so that:

- Every source row change appears on the target, with per-primary-key convergence to the newest source `ts_ns`.
- Replay-on-crash never produces duplicates (the LWW stamp absorbs them).
- Operators have visibility into replication lag + a clear operational story when something lags.
- A single consumer can keep up with real OLTP write rates (tens to hundreds of records per second per shard).

The data-plane is **asynchronous** — the target lags the source by seconds under normal load. This is not a synchronous standby and it is not transactionally consistent across rows; it's a durable, out-of-region, eventually-consistent-per-row copy.

---

## 2. Why this is non-trivial

Three properties of DSQL's CDC shape every design choice:

1. **The CDC stream is unordered across shards.** Records from one committed transaction can land on different shards and arrive in any order. There is no in-stream mechanism to recover source transaction boundaries, so the replica cannot preserve them either. The design accepts this honestly instead of engineering fiction around it.
2. **CDC is net-effect, not an operation log.** Ten updates to the same row in one transaction collapse to a single CDC record with the final value. Deletes carry only the primary key (no pre-image). A replica built on statement replay would diverge.
3. **Cross-region commits are slow.** Each DSQL `BEGIN..COMMIT` from one region to another takes ~300–500 ms. Any design that serializes commits caps at a few per second — unusable for anything beyond a demo.

---

## 3. The load-bearing primitive

Each CDC record carries a **source timestamp (`ts_ns`)** that is strictly monotonic per row at the source. The replicator uses `(schema, table, primary key, ts_ns)` as the correctness anchor: for each record, bump a stamp in `__replicator_lww_stamps` **only** if the incoming `ts_ns` is strictly greater than the stored one, and apply the row DML in the same target transaction. That one SQL rule gives:

- Per-primary-key convergence to the newest source commit.
- Idempotence on duplicate redelivery (stamp doesn't advance; row DML skipped).
- Correct behavior when an older source commit arrives late (stamp gate rejects it).

Cross-row atomicity is **not** provided — if two source commits touch two different rows, those rows can apply on the replica in any order. See the "coffee / dinner" example in `DESIGN.md §1a`.

---

## 4. The pipeline, at a glance

```
Source DSQL ──► Kinesis stream ──► Consumer process ──► Target DSQL
                                        │
                                        ├── Decode envelope + reassemble oversized records
                                        ├── Shared apply queue (per-record)
                                        ├── Apply workers: stripe-locked LWW upsert
                                        └── State plane: LWW stamps, checkpoints, leases
                                                        (lives inside target DSQL)
```

The consumer is a single long-running process. Internally it has three stages:

1. **Decode** — parse each Kinesis record, join oversized-record fragments, verify checksums.
2. **Queue** — hand each ready record (full or assembled) into a bounded apply queue, tagged with its origin shard + Kinesis sequence.
3. **Apply** — a pool of apply workers drains the queue in micro-batches. Each batch acquires per-key stripe locks for the union of touched primary keys, runs `BEGIN ... COMMIT` on target with one `INSERT ... ON CONFLICT DO UPDATE WHERE EXCLUDED.applied_ts_ns > existing.applied_ts_ns` per record (plus the row DML on the customer table), and advances the per-shard checkpoint for each contributing shard inside the same transaction.

Multiple records on disjoint primary keys apply concurrently; records sharing a primary key serialize on that key's stripe lock.

Transient apply failures (connection drops, serialization conflicts) retry forever with exponential backoff. A persistent target-side defect (schema drift, non-retryable DML error) blocks only its own stripe locks — other keys keep flowing — and surfaces on CloudWatch via a growing apply queue and replication lag.

---

## 5. State plane — all in the target cluster

The replicator's bookkeeping lives inside the target DSQL cluster itself, alongside the data it replicates. **Two tables:**

| Table | Purpose |
|---|---|
| **`__replicator_lww_stamps`** | One row per `(schema, table, primary key)` that was applied, carrying the latest source `ts_ns` seen for that key. Written as a conditional UPSERT inside the same target transaction as the row apply — the DML only runs when the incoming `ts_ns` is strictly greater than what is stored. Per-primary-key last-writer-wins; absorbs at-least-once CDC redelivery and late-arriving older commits with zero application-level logic. |
| **`__replicator_checkpoints`** | Per-shard "last-processed" Kinesis sequence number. Advanced in-band with apply for shards that contributed records to the batch; a periodic best-effort flush covers shards whose records are parked in fragment reassembly. On restart a shard worker resumes `AFTER_SEQUENCE_NUMBER` from the stored value. Row-level LWW makes any re-fetched records idempotent. |
| **`__replicator_shard_leases`** | Single-writer-per-shard coordination with a fencing token and a time-to-live heartbeat. Enables safe failover between consumer processes. |

**Why in the target cluster, not DynamoDB?** The critical correctness operation — "apply the row, bump its LWW stamp, advance the shard checkpoint" — happens in **one** target-side transaction. Nothing is distributed across services; there is no "wrote to DynamoDB but crashed before writing to DSQL" problem to engineer around.

---

## 6. The three design calls that matter

### 6.1 Per-key LWW, not transaction-level ordering

Earlier iterations tried to reassemble each source transaction across shards and release transactions in `ts_ns` order against a synthetic watermark. The DSQL service team confirmed in Nov 2026 that Kinesis cannot preserve DSQL transaction order across shards — the watermark advance was always a best-effort guess, and the ordering buffer could (and did) release a transaction before every older-`ts_ns` record had arrived. The guarantee the design *claimed* was never the guarantee it *delivered*.

The current design enforces order **per primary key** via the LWW stamp. Two records that touch the same primary key apply in `ts_ns` order; records on disjoint keys apply in any order on the replica. This is honest about what cross-shard CDC can and cannot preserve, and it maps cleanly to how industry CDC pipelines (DMS, Debezium Sink) behave — row-level eventual convergence, documented explicitly.

### 6.2 Infinite retry on transient failure, no fence

When a target-side apply fails transiently (connection drop, serialization conflict, deadlock), the applier retries that batch forever with exponential backoff capped at 10 seconds. It does **not** halt the pipeline, does **not** write a quarantine row, and does **not** require operator action for transient failures to clear.

A persistent target-side defect (schema drift, malformed record, permission error) raises `ApplyError`, logs loudly, and drops the batch so other keys keep flowing. The operator sees it on the dashboard as a growing apply queue + replication lag; CloudWatch alarms fire on both signals. There is no DLQ to drain, no fence to close, no `ops dlq retry` to run.

The trade-off: a stuck key burns CPU in the retry loop. We judge this cheaper than the operational tax of the old fence system (fences wedging forward progress on an unrelated stripe, operators debugging where to retry from).

### 6.3 Lease-based failover, not stop-the-world

For availability, you can run multiple consumer processes on the same stream. Only one process at a time owns any given shard — enforced by a lease row in the target cluster with a heartbeat and a fencing token. If a process pauses or dies, its lease expires; another process steals it, reads the stored checkpoint, and resumes from the exact sequence number of the last applied record. Any records replayed between the stored checkpoint and the actual last-applied are rejected by the per-key `ts_ns` guard on the stamp table — the stored `ts_ns` for the key is already ≥ the replay's `ts_ns`, so the stamp's `WHERE` clause skips the DML.

The fencing token means a process that pauses and later wakes up cannot accidentally resume as the shard owner — its old token no longer matches, and it shuts the shard worker down cleanly.

---

## 7. Deployment shape

The replicator ships as a single container image, deployed to AWS Fargate behind an ECS service. The surrounding infrastructure:

- A VPC with a NAT gateway for cross-region reach (the target cluster lives in a different region) and VPC endpoints for the AWS services the task talks to in-region (Kinesis, CloudWatch, SNS, ECR).
- A least-privilege IAM task role scoped to only what the replicator needs: read from one Kinesis stream, mint auth tokens for the two DSQL clusters, publish to the alarms SNS topic, write to CloudWatch metrics in one namespace.
- A CloudWatch dashboard with throughput, replication lag, apply-queue depth, lease activity, and per-shard Kinesis lag.
- Three CloudWatch alarms (apply queue near cap, replication lag, Kinesis records-behind-latest), publishing to a dedicated alarms topic.
- A customer-managed KMS key encrypting the alarms SNS topic at rest.

An operator deploys the whole thing with one command. The runbook covers rollback, log tailing, and the handful of recoveries an on-call engineer might run.

---

## 8. Edge cases the design handles

**Oversized records.** DSQL splits records larger than ~1 MB into a main record plus fragments sharing a chunk id. The decode stage buffers fragments, assembles them in order when the main lands, and verifies a checksum before passing the record on. Orphan fragments and incomplete mains both time out and are logged.

**At-least-once CDC redelivery.** A duplicate record carries the same `ts_ns` as the original. The stamp `WHERE EXCLUDED.applied_ts_ns > existing.applied_ts_ns` makes the second one a no-op.

**Late-arriving older commits.** A commit on key K with `ts_ns = 1000` that arrives after a commit on K with `ts_ns = 2000` has already applied is rejected by the stamp gate. The older state does not overwrite the newer.

**Idle shards.** A shard that goes quiet still gets its lease heartbeated and its checkpoint advanced best-effort. Idle shards do not block the apply pipeline (there is no cross-shard watermark anymore).

**Backpressure.** The apply queue has a hard cap. When the applier can't keep up, shard workers block on queue `put()`, which stops advancing the Kinesis iterator — turning Kinesis's 24-hour retention into the real overflow buffer.

**Process crash mid-apply.** The target-side transaction either commits everything (row + LWW stamp + shard checkpoint) or nothing. On restart, the shard worker resumes `AFTER_SEQUENCE_NUMBER` from the stored checkpoint; any re-fetched records are either applied (if they advance the stamp) or no-op'd (if they don't).

**Fragments in flight at crash time.** A record whose fragments were sitting in the reassembler at crash time is lost — the shard that received it checkpointed past it (best-effort) and will not re-fetch. LWW catches up on the next source write to that primary key. Operationally rare; non-fragmented rows (the vast majority of CDC traffic) are unaffected.

**Task crash under write pressure.** Lease failover (§6.3). New task resumes from checkpoint, LWW dedupes any replay.

**Schema drift (DDL on source without a matching change on target).** The applier raises `ApplyError` on the first affected record, logs it, and drops the batch. Operator applies the DDL on target; subsequent batches apply cleanly. There is no fenced payload to retry.

**Cross-region credential expiry.** Short-lived IAM tokens are cached and refreshed on a timer below their TTL; retry paths refresh on demand. Pool connections are proactively recycled before their token expires.

**Replay from Kinesis after an extended operator pause.** Kinesis retention is 24 hours by default, extendable to 7 days. The pipeline's checkpoints survive restarts; a resumed process reads from where it stopped. Beyond retention, the operator loses in-flight records — the design surfaces this via a CloudWatch metric so an alarm can fire well before data expires.

---

## 9. What's NOT covered

Intentionally out of scope, documented so there are no surprises:

- **Cross-row application atomicity.** Row-level LWW does not preserve a source transaction's multi-row invariants (foreign keys to a just-inserted parent, denormalized aggregates, "if X then Y" conditions across rows). If your application needs those, read from the source region.
- **DDL replication.** New or altered tables must be applied to both clusters by the operator. The replicator refuses to invent target schema.
- **Hot-key workloads above a few writes per second per unique row.** Cross-region commit latency is a hard physical floor at the per-key level. No parallelism fixes this.
- **Horizontal scale beyond one consumer process.** The lease model supports multiple processes for availability (failover), but running more than one simultaneously only helps when source shards ≥ children.
- **Multi-target fan-out from one process.** One source stream can feed one target cluster per consumer process. Running N targets means running N consumer processes.
- **Admin HTTPS API.** All operator actions are CLI-driven. If you want a web console, you build one on top.
- **Conflict resolution against writes made on the target cluster.** The replica is read-only; writing to it alongside replication yields undefined state.
- **Automatic KMS enablement on the Kinesis source stream.** Attaching the replicator's KMS key to a pre-existing Kinesis stream is a one-command operator step documented in the runbook, because changing stream encryption affects every producer and consumer of the stream, not just this replicator.

---

## 10. Operational properties

**What the on-call sees.** A dashboard with two single-value tiles up top (apply queue depth, replication lag), four time-series panels below (throughput, ingest errors, replication lag + queue depth, lease + checkpoint activity), and a per-shard Kinesis lag chart.

**Three alarms, feeding one topic:**
- Apply queue over 80% of its cap (apply workers can't keep up with shard workers).
- Replication lag over 10 seconds (newest applied source commit is stale).
- Any shard more than 60 seconds behind Kinesis latest.

**What the on-call does.** One of three things, in order of likelihood: (a) read the dashboard and wait for a transient to clear, (b) investigate persistent apply errors in the logs (schema drift on target, permission issue) and fix them on the target cluster — no payload replay needed because records keep flowing from Kinesis, (c) scale the service to zero as an emergency full-stop. The runbook has decision trees for each alarm.

**What the on-call does NOT need to do.** Touch DynamoDB (there is none). SSH anywhere (ECS Exec is enabled but rarely needed). Open or close fences (there are none). Run `ops dlq retry` (removed).

---

## 11. One-sentence summary of each design choice

- Per-primary-key eventual convergence works because every CDC record carries a monotonic source `ts_ns`.
- Exactly-once row effect works because the LWW stamp lives in the same target transaction as the row.
- Throughput is unlocked because records on disjoint keys apply on independent stripe lanes.
- Failover is clean because the shard lease lives next to the data its owner is writing.
- Operational surface is small because there is no fence, no DLQ queue, and no cross-shard watermark to debug.
- All of the above is simple because the state plane is one place, not three — and the semantic is honest about what cross-shard CDC can preserve.
