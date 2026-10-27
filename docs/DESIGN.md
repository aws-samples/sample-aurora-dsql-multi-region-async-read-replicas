# Design — Aurora DSQL Multi-Region Async Replicator

A prose-only companion to `OVERVIEW.md` (what it does) and `ARCHITECTURE.md`
(how the pieces fit). This document answers **why** — the design decisions
behind the implementation, the alternatives they rule out, and the trade-offs
they accept.

---

## 1. The problem

Aurora DSQL is a single-region managed database. Customers who need a
cross-region read copy, a DR standby, or a bridge during migrations have to
build the replication themselves. Doing it naively from the DSQL CDC stream
is a trap for three reasons:

- The Kinesis stream that carries CDC is **unordered across shards**. The
  rows of a single committed source transaction can land on different shards
  in different orders. Any "apply as you read" design produces partial
  transactions on the target as the normal case, not an edge case.
- CDC is **net-effect**, not an operation log. Ten updates to one row
  collapse into one record with the final value; deletes carry only the
  primary key. Statement replay would diverge from source.
- Every cross-region DSQL commit costs **~300–500 ms**. Any design that
  serializes the commit lane for the whole stream caps at a handful of
  transactions per second — unusable beyond a demo.

This project is a reference implementation that handles atomicity,
exactly-once effect, per-row order, operator recovery, and useful throughput
so customers do not have to re-derive these trade-offs.

---

## 2. What this system does — and does not do

**Does:**

- Mirrors committed row changes from one Aurora DSQL cluster to another
  DSQL cluster in a different region (or stage), asynchronously, with
  seconds of lag under normal load.
- Reassembles each source transaction completely before anything downstream
  sees it. Target readers never observe partial transactions.
- Guarantees exactly-once effect under replay. Duplicate deliveries,
  consumer restarts, and operator retries produce the same target state.
- Preserves per-row commit order. Two transactions touching the same row
  commit in source order; two transactions on disjoint rows may commit
  concurrently.
- Routes permanently-failing transactions to a durable fence stored in the
  target cluster. Forward progress halts cleanly until an operator inspects,
  retries, or explicitly skips.
- Fails over cleanly between consumer processes via a shard lease with a
  fencing token and a heartbeat.
- Publishes throughput, lag, buffer depth, fence state, and per-shard Kinesis
  lag to CloudWatch with a dashboard and five alarms.
- Deploys as a single container on AWS Fargate with one command, encrypted at
  rest, under a least-privilege IAM role.

**Does NOT:**

- Replicate DDL — the operator applies schema changes to both clusters out of
  band.
- Preserve cross-row application-level atomicity across the replica (a
  two-account money transfer is not atomic from the replica's view). This
  matches standard CDC behavior.
- Resolve conflicts against writes made on the target cluster.
- Filter or transform rows.
- Provide an HTTPS admin surface — all operator actions are CLI-driven.
- Run more than one active consumer process at a time. The lease model is
  for failover, not throughput.
- Fan one source out to multiple target clusters from one process.

---

## 3. The design choices

Each entry: what we decided, why we decided it, what we traded away.

### 3.1 Reassemble every transaction before any downstream stage sees it

The CDC envelope carries a transaction id and a total-records count on every
record of a transaction. The pipeline buffers records keyed by transaction id
and releases only when the count matches.

- **Why.** Without this, Kinesis's cross-shard unorderedness makes
  partial-transaction apply the normal case. This invariant is the
  load-bearing primitive that makes atomic cross-row apply downstream
  possible.
- **Trade-off.** Memory held per in-flight transaction, plus a timeout path
  for transactions whose records never fully arrive. The timeout surface is
  explicitly tested; we do not rely on it in production.

### 3.2 Per-shard ordering with a bounded priority buffer

Completed transactions sit in a priority queue keyed by their source
timestamp. The queue releases a transaction only when every shard's
watermark has advanced past it, using a strict less-than rule. The buffer has
a hard cap; when full it back-pressures the Kinesis reader.

- **Why.** Out-of-order release across shards would commit a newer timestamp
  before an older one and break the per-key ordering guarantee downstream.
  The strict less-than rule prevents equality collisions where two shards
  happen to carry the same timestamp. The hard cap prevents unbounded memory
  as a failure mode when the applier is slower than the consumer.
- **Trade-off.** One extra ingest tick of latency at the gate. Under
  sustained back-pressure, Kinesis's 24-hour retention becomes the overflow
  buffer.

### 3.3 Per-key stripe-locked parallel apply (not serial, not per-row)

The applier owns N stripes. Each transaction hashes its primary keys into a
sorted set of stripes, acquires those stripes' locks in sorted order, then
runs one atomic target-side transaction holding only those stripes.
Transactions on disjoint keys commit concurrently; transactions sharing any
key serialize on that key.

- **Why.** An earlier design serialized the entire commit lane for a global
  timestamp order and capped at a handful of transactions per second —
  unusable for any realistic workload (OLTP commonly runs 50–500 TPS).
  Per-row order is the contract SQL already gives per row; cross-key order
  was conservative, not required. Industry CDC pipelines (DMS, Debezium
  Sink, Striim, Fivetran) all do this for the same reason.
- **Trade-off.** Cross-row application-level atomicity across the replica is
  lost. The system now matches the standard CDC contract rather than
  offering a stronger one.

### 3.4 Idempotent apply via per-key last-writer-wins stamps in the target database

*Note: see the 2026-10-26 LWW section below for the current design. The
historical description preserved here captures the original single-stamp
intent; the invariant has since been strengthened to per-key `ts_ns`
LWW to absorb at-least-once CDC redelivery.*

The target cluster holds a stamp table keyed by `(schema_name, table_name,
primary_key)` carrying the latest source `ts_ns` applied for that key. Each
row apply is a conditional UPSERT inside the same target transaction as
the row DML — the DML runs only when the incoming `ts_ns` strictly exceeds
the stored one. Replays see the stamp and short-circuit.

- **Why.** Any other scheme creates a window where "crashed between apply
  and remembering we applied" produces a duplicate or a loss. Putting the
  stamp outside the target database (e.g., DynamoDB) makes stamping
  non-atomic with the row and turns recovery into a correctness problem
  rather than a bookkeeping one.
- **Trade-off.** The stamp table grows forever unless pruned (acceptable at
  current scale), and one extra SELECT per record on replay.

### 3.5 State plane co-located in the target DSQL cluster

Six tables — applied-stamps, DLQ fence, DLQ payload, checkpoints, shard
leases, watermark state — all live inside the target DSQL cluster alongside
the user data.

- **Why.** Every correctness-critical state transition ("apply the row, mark
  it applied, close the fence that covered it, advance the checkpoint") can
  happen in one target-side transaction. There is no distributed-state
  coordination problem to engineer around. One IAM boundary, one failure
  domain.
- **Trade-off.** Writes to the target include replicator overhead. Losing
  the target cluster means losing the state plane. The replicator's
  reliability is bounded by the target cluster's.

An earlier design kept checkpoints and watermark state in DynamoDB and only
moved DLQ into target DSQL. That contradicted the stated principle "state
lives where the applier runs" and introduced cross-service consistency logic
for the lease. We reversed it, consolidated to six tables in the target
cluster, and deleted the DynamoDB infrastructure.

### 3.6 Single-writer per shard, enforced by a fencing-token lease

Only one Fargate task actively owns a shard at a time, enforced by a lease
row in the target cluster with a monotonic token and a periodic heartbeat.
The service runs at `desiredCount=1`; the lease is for failover, not
horizontal scale.

- **Why.** Two tasks each with their own commit lane could commit
  transactions out of order relative to each other and break the per-key
  ordering guarantee. A shared cross-task commit lane via an advisory lock
  just serializes everything anyway. The fencing token makes stale wake-ups
  safe: a paused-then-resumed process finds its token no longer current and
  shuts its shard worker down cleanly.
- **Trade-off.** Per-process throughput is the system's throughput ceiling.
  Horizontal scale would require weakening the cross-shard ordering
  guarantee or introducing cross-task coordination; neither is shipped.

### 3.7 DLQ "fence" as a durable, inspectable per-transaction quarantine

When a transaction fails permanently to apply (schema mismatch, constraint
violation, unknown table), the applier writes one fence row to the target
cluster and a payload row preserving the whole transaction. The ordering
stage refuses to release any transaction past the fence timestamp until an
operator closes it. Retry re-applies the stored payload and closes the fence
inside one target transaction.

- **Why.** Silently skipping diverges the target. Retrying forever loses
  forward progress. Crashing the process is operator-hostile and loses
  in-flight state. A durable, inspectable fence pinned to the exact failing
  transaction is the only option that preserves correctness and gives
  operators a one-command recovery. Keeping the payload in the target means
  retry is a pure target-side operation — no Kinesis replay needed.
- **Trade-off.** The current fence is **global** — any one failure halts
  all forward progress. Per-primary-key quarantine (hold only transactions
  touching the fenced keys) is a possible extension. Today's behavior is
  correct, just more disruptive than it needs to be.

### 3.8 DDL is explicitly not replicated

The applier assumes schemas exist on both sides. When it hits an unknown
table or column, it raises an apply error, which opens a fence; the operator
applies the DDL on the target and runs retry.

- **Why.** Inferring DDL from CDC is brittle. Column renames, type changes,
  index creation, triggers, and defaults all fail in different ways. Getting
  it wrong on a production replica is worse than making the operator
  responsible.
- **Trade-off.** Schema changes become a two-step operator process. In
  return, the replicator never invents target schema and never silently
  diverges on DDL drift.

### 3.9 Source → Kinesis → target topology (not direct DB-to-DB)

The replicator reads a Kinesis stream owned by the customer (fed by DSQL CDC
in the source region) and commits to a target DSQL cluster in a different
region.

- **Why.** Kinesis is the DSQL CDC emission surface — there is no direct
  outbound CDC stream from DSQL. Kinesis also gives durable buffering during
  replicator downtime (24-hour retention, extendable to 7 days) and a
  natural fan-out point for additional consumers.
- **Trade-off.** Source-to-target latency includes CDC emission lag,
  Kinesis delivery, and cross-region commit. In practice this runs
  single-digit seconds under normal load.

### 3.10 IAM-token auth with per-connection reconnect on expiry

Both Kinesis and both DSQL clusters authenticate via IAM-generated
short-lived tokens (~15 minutes). Every long-running connection — state
connection, DLQ writer, schema cache — wraps each operation in a one-retry
loop that reconnects on expired-token errors. The container also writes a
liveness heartbeat file that the healthcheck reads.

- **Why.** DSQL does not offer long-lived credentials. An earlier revision
  had reconnect only on the applier's commit lane; stress testing exposed
  that the state connection wedged silently after ~35 minutes when its
  token expired, while the container healthcheck still passed because it
  opened fresh connections. We added per-connection reconnect everywhere
  plus the liveness heartbeat so any future wedge — not just token expiry —
  fails the healthcheck.
- **Trade-off.** Every long-lived connection carries its own reconnect
  logic. The alternative (a centralized token refresher) is less local and
  more error-prone.

### 3.11 Fargate single-task deployment with CloudWatch alarms (not Lambda, not EKS)

The replicator runs as one Fargate task behind an ECS service with the
deployment circuit breaker enabled. The task role is scoped to one Kinesis
stream, two DSQL cluster ARNs, two SNS topics, and one CloudWatch namespace.
Five alarms (apply failures, open fence, ordering buffer near cap, watermark
lag, Kinesis records-behind-latest) publish to a dedicated SNS topic. A
customer-managed KMS key encrypts both SNS topics at rest.

- **Why.** Lambda's execution limits make long-running CDC consumption
  impractical. EKS adds a cluster to operate for one container. Fargate is
  the lightest shape that gives a long-running task, native CloudWatch
  integration, and healthchecks. One task matches the single-writer-per-
  shard contract.
- **Trade-off.** Monthly cost (dominated by the NAT gateway and interface
  endpoints). Horizontal scale would require weakening the ordering
  contract first.

---

## 4. What these choices solve, by failure class

**Correctness.** Duplicate deliveries and operator retries never produce
duplicate rows on the target (per-transaction stamp table). Partial
transactions never become visible to target readers (reassembly before
release + one-transaction atomic apply). Out-of-order delivery within a
transaction is absorbed by the grouping stage; out-of-order delivery across
shards is absorbed by the ordering stage.

**Liveness.** A stuck transaction opens a durable fence rather than crashing
the process or looping forever. Expired IAM tokens trigger an in-process
reconnect on the next operation. An operator-initiated pause reuses the
fence mechanism, so there is one halting primitive, not two. Idle shards get
a synthetic watermark advance when Kinesis itself reports they are caught
up, so a quiet shard does not freeze the pipeline. Ordering-buffer
back-pressure stops advancing the Kinesis iterator rather than silently
dropping records.

**Operability.** Failover is clean: the shard lease lives next to the data
its owner is writing, with a heartbeat and a fencing token. Lag visibility
comes from a dashboard with single-value tiles for the four signals an
on-call reads first, plus five alarms with decision trees in the runbook.
The blast radius of a bad write is bounded by the fence — forward progress
halts, nothing silently diverges. The runbook explicitly tells on-call what
NOT to do: there is no DynamoDB to touch, SSH is rarely needed, Kinesis
replay is unnecessary because the stored payload makes retry a pure
target-side operation.

**Scaling.** Per-key parallel apply unlocks throughput for workloads whose
keys are reasonably distributed (measured roughly 6× transaction and 12× DML
improvement over the prior serial lane). Hot-key workloads cap at ~3
updates/second per unique primary key — cross-region commit latency is
physics, not software. Backlog grows linearly, not exponentially, under
sustained over-load and drains after load stops. The system is stable under
over-provisioning, not runaway.

---

## 5. Known limits

- **Steady-state apply ceiling** on the single-task shape is ~250 TPS
  cross-region. Above that, lag grows linearly and drains post-load.
  Multi-task scale with weakened cross-shard ordering is a possible
  extension; not shipped.
- **Hot-key workloads** cap at ~3 updates/second per unique primary key.
- **DDL is not replicated** — operator applies schema changes to both
  clusters out of band.
- **Service runs at `desiredCount=1`.** The lease model is for failover, not
  throughput.
- **Historical rows written before the replicator was started cannot be
  bridged** — Kinesis retention is 24 hours (extendable to 7 days). The
  design surfaces this as a CloudWatch metric so an alarm fires before data
  is lost.
- **Cross-row application-level atomicity across the replica is not
  preserved** — only per-row order. This matches standard CDC behavior.
- **One source feeds one target per consumer process.** Fan-out to multiple
  targets means running multiple processes.
- **No HTTPS admin surface** — all operator actions are CLI-driven.
- **Source Kinesis stream's KMS encryption** is a one-time operator step on
  prod cutover, not a CDK takeover, because changing stream encryption
  affects every producer and consumer of the stream.

---

## 6. Key reversals during design

Each of these is a design iteration driven by an observed failure, not an a
priori preference. They are the parts of the design most worth challenging.

1. **Global timestamp ordering → per-key stripe-locked apply.** A 15-minute
   100 TPS load test proved the serial-applier ceiling at ~5–7 DMLs/sec on
   cross-region commits. Insufficient for any real OLTP workload. The
   reversal weakened the ordering contract to per-row (what SQL guarantees
   anyway) and unlocked the throughput ceiling.

2. **State plane in DynamoDB → state plane in target DSQL.** An earlier
   design kept checkpoints and watermark state in DynamoDB and only moved
   DLQ into target DSQL. That contradicted "state lives where the applier
   runs" and introduced cross-service consistency logic. The reversal
   consolidated to six tables in the target cluster.

3. **Narrower task role → kept the slightly broader DLQ permission.** A
   stricter "list-only" variant for the DLQ SNS topic tripped the ECS
   deployment circuit breaker during the rollout window where the new
   (narrower) task role rolled out before the new image. The idempotent
   create permission is kept because tightening it broke rolling deploys.

4. **Applier-only reconnect → every long-lived connection reconnects.** A
   500 TPS stress test exposed that the state connection wedged silently
   after ~35 minutes when its IAM token expired. The container healthcheck
   still passed. Per-connection reconnect everywhere, plus a
   liveness-heartbeat file so any future wedge — not just token expiry —
   fails the healthcheck.

---

## 2026-10-26 — Measurement, incidents, and the applier ceiling

### What landed this day
1. **Cross-shard transaction reassembly** (`6187127`): a `TransactionReassembler`
   that lived per-shard worker silently dropped ~90% of transactions on
   multi-shard streams. CDC distributes a single source transaction's records
   across every open shard, so each per-shard reassembler timed out with
   `buffered < expected`. Fix: one process-global reassembler plus a dedicated
   sweeper task that emits `TxnTimeouts` so this failure mode is never silent.
2. **Micro-batching (pattern A)** (`6187127`): `Applier.apply_batch(N)` packs N
   source txns into one target `BEGIN...COMMIT`. Config knobs
   `BATCH_MAX_TXNS` / `BATCH_MAX_WAIT_MS`. Fence/DLQ granularity stays
   per-source-txn via a per-txn fallback on batch failure.
3. **Multi-process supervisor (pattern C)** (`6187127`): `REPLICATOR_PROCESSES`
   spawns N child consumer processes; children race for shard leases via the
   existing table. CDK bumped 4 vCPU / 8 GB → 8 vCPU / 16 GB.
4. **SSL-EOF connection-storm fix** (`89a4907`): `DsqlTokenCache` grew an
   `asyncio.Lock`, moved SigV4 signing to a thread, and `_replace_conn`
   retries open with backoff instead of handing back a known-broken socket.
   Pool pre-warm staggers 20 ms per connection.
5. **Load-gen credential rotation fix** (`6286c6f`): recreate `boto3.Session`
   per token refresh, not just the client, so long runs survive ada rotation.
6. **Dashboard SEARCH aggregation** (`5c07676`): widgets now use
   `SUM/MAX(SEARCH(...))` so the dashboard works for `REPLICATOR_PROCESSES >= 1`
   without rewriting per deploy. Prior widgets filtered on fixed dim sets and
   went silent once the ProcessId dim was introduced.

### Measurement outcome

Source: 300 async workers, 30-min duration, 5-min ramp, source cluster
`kbud4lp76tyvoj5zligoi5otlm` → Kinesis `my-cdc-stream` (4 shards). Target:
`djue5mkorsu6v67bw4jise6qna` (us-east-2).

- **Source**: 952,734 txns committed in 30 min, zero failures, effective
  ~529 TPS; p50/p95/p99 at 459 / 889 / 1,226 ms.
- **Applier**: steady 87–93 TPS. No fences. No apply errors. Zero retries
  observed in logs.
- **p1 process**: 0 txns throughout. See "Open limits" below.

### Open limits (not fixed this day)

**Target-DSQL concurrent-write ceiling.** The applier plateaued at ~88 TPS
and did **not** move with any client-side knob:

| Change | Expected (theory) | Observed |
|---|---|---|
| `BATCH_MAX_TXNS=16 → 2` | ~500 TPS from stripe-overlap relief | 11 → 88 TPS |
| `APPLIER_WORKERS=128 → 256` | ~2× if connection-bound | **unchanged** |

Signals that argue against client-side:
- Fargate CPU: <20 % of **one** vCPU (task has 8).
- Connection pool: 118+ idle slots at all times.
- Per-batch `stripes_count`: ~2 of 512 (overlap <1 %).
- Per-batch commit `duration_ms`: 187-290 (healthy cross-region).
- Zero fences, zero `_RETRYABLE` log lines.

Working hypothesis: target DSQL enforces a per-cluster concurrent-commit
cap (around 10 in-flight writes) that is not surfaced by connection or
QPS quotas. Needs an AWS case to confirm; alternately, a side-by-side
run against a fresh target cluster to see if the cap moves. Pending.

**Multi-process shard starvation.** `REPLICATOR_PROCESSES=2` with 4 shards:
p0 wins all 4 leases first, p1 idles. The GIL relief multi-process is meant
to buy only works when `shards >= processes`. Horizontal task scale with
deterministic shard partition `shard_hash % N == task_id` is the general
fix; not shipped.

### Lessons for next time

- When a server-side error correlates perfectly with cold-start client
  bursts (`SSL EOF`, `access denied`, `connection reset by peer`), look
  for an **unlocked boto3 call** on the hot path before blaming the
  server. SigV4 signing calls at 128× concurrency saturate client-side
  resolver code and present as server errors.
- Monitor **both** source and dashboard widgets' dim schema when a feature
  touches metrics emission. Dim mismatch is silent on both ends.
- Hot-swap task-def env vars for one-knob experiments; faster than a
  full `cdk deploy` and preserves the running backlog.
- Validate the pool warm-up stagger with a 128-worker cold start before
  concluding a connection-limit theory.

### 2026-10-26 — Follow-up: the "DSQL cap" was actually stripe-lock contention

The initial writeup above hypothesized a target-DSQL concurrent-write cap
to explain the 88-TPS plateau. **That was wrong.** Per-stage timing added
after the writeup (commit `29850a3`) revealed the real cost sink:

| Stage | p50 | p95 | max |
|---|---|---|---|
| schema_ms | 0.1 | 0.1 | 0.1 |
| pool_wait_ms | 0.0 | 0.0 | 0.0 |
| commit_ms | 230 | 327 | 327 |
| **stripe_ms** | **842** | **24,583** | **31,773** |

The applier was spending **24+ seconds at p95 just waiting to acquire the
asyncio stripe locks** before a batch could begin. `duration_ms` in the
`apply.ok` log timers only the commit itself — it starts AFTER stripes are
held — so this wait was invisible in every earlier observation.

Why Fargate CPU looked idle: `asyncio.Lock.acquire()` is pure sleep. The
process was waiting, not doing work.

**Collision math.** `BATCH_MAX_TXNS=2` × ~3.5 records/txn → ~7 stripes per
batch. With 512 stripes and 128 workers:

- P(two batches disjoint) = (505/512)^7 ≈ 0.91
- P(a new batch collides with any of K in flight) = 1 − 0.91^K
- At K=128 the collision probability is essentially 1.0.

Every new batch queued behind at least one in-flight batch; cumulative
queue depth × commit_ms matched the ~24 s p95 we observed.

**Fix.** `APPLIER_STRIPES=512 → 8192`:

- P(disjoint) → (8185/8192)^7 ≈ 0.994
- P(collision at K=128) → ~54 % → p99 ~1 s instead of ~30 s

Measured result on task-def `:22`:

- stripe_ms p50 **842 → 0.0 ms**, p95 **24,583 → 556 ms**
- Fargate CPU **190 → 984 CpuUnits** (idle → ~1 full vCPU of real work)
- Applier TPS **88 → ~700 TPS** steady, over source target of 600

**CDK default updated** to `APPLIER_STRIPES=8192`.

**Lesson.** The invariant for `APPLIER_STRIPES` is: it must be ≫
`APPLIER_WORKERS × (max stripes per batch)` or workers queue on locks and
the pipeline floor is the serial RTT. Rule of thumb: stripe count = 64 ×
worker count, minimum. The 2026-10-14 build had `applier_stripes = 512,
applier_workers = 32` — ratio of 16× — which happened to work at low
concurrency; today's 128-worker shape crossed the threshold where it
collapses.

**Credit.** User pushed back on the "server-side cap" hypothesis before I
had evidence. Instrumentation then confirmed the client-side pathology.
Lesson for future debugging: **time every stage, don't just time the
commit** — invisible wait inside asyncio locks is a classic Python
performance footgun.

---

## 2026-10-26 — Per-key last-writer-wins by source `ts_ns`

### Why the earlier design was insufficient

The previous `__replicator_applied(tx_id, record_id)` dedup correctly
blocked **exact duplicate** redelivery (same `tx_id`, same `record_id`
replayed from Kinesis after a crash). It did NOT handle two cases that
the DSQL CDC team flagged as part of their at-least-once contract:

1. **Duplicate redelivery of a newer txn after we've already applied
   an older one on the same key.** CDC may deliver `T2` (`ts_ns=200`),
   then later deliver `T1` (`ts_ns=100`), then re-deliver `T2`. The
   service team's design assumes the consumer applies the final `T2`
   so the state converges. Our old dedup would skip the second `T2`
   as a duplicate `(tx_id, record_id)`, leaving the target stuck at
   `T1`'s stale value.

2. **Late-arriving older txn on a key we've already written.** If the
   watermark advances past `ts_ns=200` (e.g. via idle-shard synthetic
   advance) and `T1@ts_ns=100` arrives afterward, the old design would
   apply `T1` unconditionally, overwriting `T2`'s newer value.

Both failure modes end in silent per-key divergence.

### What we changed (commit `9cf947e`)

The bookkeeping table is now:

```sql
CREATE TABLE __replicator_applied_v2 (
    schema_name   text   NOT NULL,
    table_name    text   NOT NULL,
    pk            text   NOT NULL,  -- JSON array of PK columns
    applied_ts_ns bigint NOT NULL,
    applied_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, table_name, pk)
);
```

Per-record application does a **conditional UPSERT** inside the same
target transaction as the row DML:

```sql
INSERT INTO __replicator_applied_v2
    (schema_name, table_name, pk, applied_ts_ns)
VALUES ($s, $t, $pk, $ts)
ON CONFLICT (schema_name, table_name, pk) DO UPDATE
    SET applied_ts_ns = EXCLUDED.applied_ts_ns,
        applied_at    = now()
    WHERE __replicator_applied_v2.applied_ts_ns < EXCLUDED.applied_ts_ns;
```

The applier checks `cur.rowcount`:

- `1` → the stamp advanced; run the row DML.
- `0` → the stamp was gated by the WHERE clause (incoming `ts_ns` is
  not strictly greater than stored); skip the DML.

Because the stamp and the DML share the single target `BEGIN...COMMIT`,
the pair is atomic: either both land or neither does. On rollback,
nothing persists; on retry, the stamp lookup still shows the correct
latest `ts_ns`.

### Invariant now guaranteed

> For every replicated key, the value in the target cluster is always
> the one from the source transaction with the highest `ts_ns` the
> replicator has ever applied for that key, regardless of delivery
> order, duplicate redelivery, or late-arriving older transactions.

### What it also fixes beyond CDC redelivery

- **Replay after process crash**: the stamp carries the `ts_ns` of the
  previously-applied txn; replay sees stored `ts_ns ≥ replayed`, UPSERT
  is a no-op, DML is skipped. Previously handled by `(tx_id, record_id)`;
  now handled by the key-level `ts_ns` guard without needing `record_id`
  to be stable across retries.

- **Watermark-skipped older txn (idle-shard misclassification)**: if the
  synthetic advance wrongly declares a shard caught up and the pipeline
  releases a late-arriving `T1@100` after `T2@200` has committed, the
  stamp for the key already has `ts_ns=200`; `T1`'s UPSERT is gated out;
  DML never runs. The target stays at `T2`'s value — no silent divergence.

### What this costs

- One extra UPSERT per record (same per-record cost as the prior design;
  wider row and a WHERE clause but still single-roundtrip inside the
  batch's target transaction).

- `__replicator_applied_v2` grows with the count of **distinct keys ever
  touched**, not the count of txns. For most workloads this is strictly
  smaller than the old table and bounded by the user's row count. A
  TTL / periodic compaction job can prune ancient entries without
  affecting correctness as long as the compaction window exceeds the
  longest expected CDC redelivery delay.

### Opt-in alternative (not shipped yet)

For hot tables where even the bookkeeping lookup is unwanted, users can
add a `_replicator_t_commit timestamptz` column to the base table and
the replicator can gate the UPDATE directly on it. More intrusive and
not yet shipped; path A (bookkeeping) is the default and sufficient.

### Migration

The old `__replicator_applied(tx_id, record_id)` table is left in place
(unused). It can be dropped manually after a confirmed clean run on the
new build. No automatic migration is attempted — the two tables answer
different questions, and the new one starts empty.

### Late-discard observability (shipped alongside the LWW change)

A late older `t_commit` on a key we've already written isn't an error — it's
the LWW invariant doing its job. But a silent drop with no audit trail hides
information operators sometimes need: *was this record ever seen?*, *how far
behind was the slow shard?*, *which business entity lost a write?*.

The 2026-10-26 LWW bundle therefore also added:

1. **`__replicator_late_discards(tx_id, record_id, schema_name, table_name,
   pk, incoming_ts_ns, stored_ts_ns, op, payload, discarded_at)`** —
   written inside the batch's open target transaction so the audit row is
   atomic with the LWW stamp decision. `ON CONFLICT (tx_id, record_id) DO
   NOTHING` makes the write idempotent across retries. `payload` is the
   full `{before, after}` JSON so operators can reconstruct what the
   losing write would have done.

2. **Structured log line `apply.late_discard`** on every late-discard
   event, with `tx_id`, `record_id`, key, incoming/stored ts_ns, delta_ms,
   op. One grep in CloudWatch Logs Insights surfaces every losing write.

3. **New CloudWatch metric `RowsLateDiscarded`** on the dedicated counter
   `ConsumerPipeline.rows_late_discarded`. Separate from `RowsDeduped`
   (crash-replay) and from `ApplyErrors` / `FencesOpened` (genuine
   failures). The dashboard's throughput panel shows it as its own series
   so a spike is immediately visible without being conflated with normal
   dedup activity.

**Implementation note:** when the LWW stamp's `WHERE` gate rejects the
UPSERT, `_stamp()` does one extra `SELECT applied_ts_ns FROM
__replicator_applied_v2 WHERE …` to recover the stored value. This lets
the caller distinguish `stored_ts > incoming_ts` (late discard, logged
and persisted) from `stored_ts == incoming_ts` (crash-replay dedup,
counted but not persisted — the record was already applied under its
own `tx_id`). The extra SELECT only runs on the non-advance path, so
happy-path cost is unchanged.
