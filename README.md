# Aurora DSQL Multi-Region Async Replicator

A Python + CDK + Fargate reference implementation that reads Aurora DSQL CDC
from Kinesis in one region and applies it to a target DSQL cluster in another
region with **row-level eventual consistency**.

Each record upserts the row and bumps a `(schema, table, pk, applied_ts_ns)`
stamp in `__replicator_lww_stamps`, gated by
`WHERE EXCLUDED.applied_ts_ns > existing.applied_ts_ns`. Rows converge to
the newest source commit per primary key; cross-row transactional invariants
are **not** preserved on the replica. Scales within a single Fargate task
via per-key stripe-locked parallel apply (128 workers / 8192 stripes),
infinite retry on transient apply failure, fencing-token lease for clean
failover, and multi-process children (`REPLICATOR_PROCESSES=2`) for GIL
relief.

### Row-level eventual consistency (what this means in practice)

Kinesis does not preserve DSQL transaction order across shards. The old
cross-shard reassembly + ordering buffer + DLQ fence machinery looked like
it provided transactional consistency on the replica but did not — it
could still release a transaction before every older-ts_ns record had
arrived, and the per-key LWW stamp was doing the real work anyway. The
2026-11 simplification pass removed that machinery and made the semantic
honest.

**Caleb's coffee / dinner example.** Suppose source transaction T1 writes
row `coffee.id = 42` (ts_ns = 1000) and T2 writes row `dinner.id = 7`
(ts_ns = 2000) — two separate rows, two separate source commits. Kinesis
can deliver T2's record before T1's. The replica will briefly show
`(coffee@original, dinner@T2)` — a state that **never existed at the
source** — until T1's record arrives and advances `coffee.id = 42`.

The guarantee is: for any primary key, the replica eventually converges
to the newest source `ts_ns`. The non-guarantee is: cross-row invariants
held by a source transaction (foreign keys that reference newly-inserted
parents, denormalized aggregates, "if X then Y" multi-row conditions)
can be observed violated on the replica between the time one row applies
and the other does. If your application needs those, read from the
source region.

---

## Reading order

0. **`docs/design-tour.html`** — interactive single-page tour of the design
   with step-through scenarios for each of the correctness-critical paths
   (atomic checkpoint, cross-shard reassembly, LWW race, ingest-DLQ, apply
   failure + fence). Open the file directly, or view online via
   [raw.githack](https://rawcdn.githack.com/aws-samples/sample-aurora-dsql-multi-region-async-read-replicas/main/docs/design-tour.html).
0a. **`docs/DEPLOY.md`** — the single page an operator reads to go from clone
   to replicating. Prereqs → CDC enablement → bootstrap → both deploy modes →
   verify → teardown → troubleshooting.
1. **`docs/OVERVIEW.md`** — code-free architecture walkthrough.
2. **`docs/DESIGN.md`** — the design decisions, with the why and the
   trade-off accepted for each.
3. **`docs/ARCHITECTURE.md`** — ASCII diagrams, component map, sequence flows.
4. **`docs/RUNBOOK.md`** — on-call operations: dashboard tour, alarm decision
   trees, deploy/rollback, state-plane recipes.
5. **`replicator/README.md`** — the Python package (install, test, CLI).
6. **`infra/README.md`** — the CDK stack (deploy / destroy).

---

## Repository layout

```
.
├── README.md ← you are here
├── docs/
│ ├── design-tour.html ← interactive scenario-by-scenario tour
│ ├── DEPLOY.md ← day-one deploy: clone → replicating, one page
│ ├── OVERVIEW.md ← code-free architecture (start here)
│ ├── DESIGN.md ← design decisions + known limits
│ ├── ARCHITECTURE.md ← diagrams + sequence flows
│ └── RUNBOOK.md ← on-call ops guide
│
├── replicator/ ← Python package: the replicator itself
│ ├── pyproject.toml
│ ├── Dockerfile ← tini + non-root + HEALTHCHECK
│ ├── README.md
│ ├── justfile ← dev shortcuts (lint, test, build)
│ ├── src/dsql_replicator/
│ │ ├── cli.py ← click entry points: migrate | consume | healthz
│ │ ├── config.py ← env-driven Config dataclass
│ │ ├── envelope.py ← CDC envelope parser
│ │ ├── reassembler/ ← fragment reassembly (>1 MB rows)
│ │ ├── applier/ ← stripe-locked parallel LWW apply + schema cache
│ │ ├── state/ ← checkpoints + shard leases
│ │ ├── consumer/ ← end-to-end pipeline + metrics loop + liveness
│ │ ├── migrate/ ← CloudWatch dashboard upsert
│ │ ├── metrics.py ← CloudWatch emitter
│ │ └── health.py ← healthz probes (DSQL + liveness file)
│ └── tests/ ← pytest unit tests
│
├── infra/ ← AWS CDK Python deployment
│ ├── app.py
│ ├── cdk.json
│ ├── requirements.txt
│ ├── README.md
│ └── stacks/
│ └── replicator_stack.py ← single-stack: VPC + endpoints + NAT + ECR asset +
│ KMS + alarms SNS topic + 3 alarms + Fargate task + service
│
└── loadtest/ ← all load / test tooling (see loadtest/README.md)
  ├── README.md ← which script for which scenario
  ├── synthetic_publisher.py ← direct Kinesis publisher (full ramp,
  │ chunked, parse / CRC / LWW injection)
  ├── _setup_demo_schema.py ← creates `loadtest_events` on the target
  ├── generate_load.py ← async DSQL-source load generator with ramp-up
  ├── schemas.sql ← its DDL
  ├── pg_tps.py ← Postgres-source steady-rate loader
  ├── pg_batch_load.py ← Postgres-source batched loader
  ├── pg_customers_load.py ← Postgres-source customer backfill
  ├── pg_load_1m.sql, pg_append_5m.sql ← Postgres seed + append
  └── dsql-cdc-tail.py ← tail a CDC stream in your terminal
```

---

## Build + test (local)

```bash
cd replicator/
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'

ruff check . && ruff format --check .
mypy
pytest -q
```

---

## Deploy

> **➡️ Full end-to-end deployment guide: [`docs/DEPLOY.md`](./docs/DEPLOY.md)** —
> single page, clone to replicating in ~20 minutes. Covers prereqs, DSQL CDC
> enablement, workspace setup, state-plane bootstrap, both deploy modes,
> traffic generation, verification, teardown, and troubleshooting.

Short version (clone + bootstrap + deploy):

```bash
cd infra/
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

AWS_REGION=<control-region> .venv/bin/cdk bootstrap
.venv/bin/cdk deploy \
 -c source_cluster_id=<SOURCE_CLUSTER_ID> \
 -c source_region=<source-region> \
 -c target_cluster_id=<TARGET_CLUSTER_ID> \
 -c target_region=<target-region>
```

Two modes for naming the source are available — `source_cluster_id` (preflight
auto-derives the Kinesis stream from the cluster's CDC config) or
`source_stream_name` (consume a Kinesis stream directly). See
[`docs/DEPLOY.md`](./docs/DEPLOY.md) for which to pick and why.

Prereq: Docker Desktop running (CDK builds the image locally).

---

## Known limits

- **Row-level, not transaction-level, consistency.** Cross-row invariants
  held by a single source transaction (foreign keys to a just-inserted
  parent, denormalized aggregates, "if X then Y" multi-row conditions)
  can be observed violated on the replica between the application of
  one row and the other. The replica eventually converges per primary
  key — rows do, transactions don't. See the "Caleb's coffee / dinner"
  example above.
- **Fragments (>1 MB rows) spanning shards can lose a single row on crash
  before the next source write.** The fragment reassembler is
  process-global and holds fragments in memory until the main record
  assembles. A task crash with fragments still buffered loses that row
  until the next source write to the same primary key, which LWW will
  then apply. Operationally rare; non-fragmented rows (the vast
  majority of CDC traffic) are not affected.
- **Infinite apply retry** burns CPU on a persistent target-side defect
  (e.g., schema drift). Alarm on `ApplyQueueNearCap` and
  `ReplicationLag` — a stuck key will show up as a growing queue and
  stale lag before anything else goes wrong.
- **`APPLIER_STRIPES` must be ≫ `APPLIER_WORKERS × avg_stripes_per_batch`**
  or workers queue on asyncio stripe locks and the applier floor collapses
  to the serial RTT. Rule of thumb: `STRIPES ≥ 64 × WORKERS`. CDK default
  is now 8192 for 128 workers. See `docs/DESIGN.md` for the derivation.
- **Multi-process shard-starvation**: with `REPLICATOR_PROCESSES > 1` and
  shards ≤ children, the first process to acquire a shard lease keeps it;
  siblings sit idle. The GIL-relief multi-process is intended to deliver is
  only realized when source shards ≥ children. Horizontal task scaling is
  the general fix (deterministic shard partitioning per task).
- **Hot keys**: capped at ~3 updates/sec per unique primary key (cross-region
 commit floor).
- **DDL not replicated**: operator must apply schema changes to both clusters.
- **Single consumer process at a time** (`desiredCount=1`): the lease model
 is for failover, not throughput.
- **Historical rows pre-dating the replicator cannot be bridged** (Kinesis
 default retention is 24h, extendable to 7 days).

Full list in `docs/RUNBOOK.md` and `docs/OVERVIEW.md`.

---

## License

Apache License 2.0 — see [`LICENSE`](./LICENSE).
