# Aurora DSQL Multi-Region Async Replicator

A Python + CDK + Fargate reference implementation that reads Aurora DSQL CDC
from Kinesis in one region and applies it atomically to a target DSQL cluster
in another region.

Correctness-first: per-transaction reassembly, cross-shard global
transaction reassembly, per-key last-writer-wins via a `(schema, table, pk,
applied_ts_ns)` stamp table in the target cluster (absorbs at-least-once
CDC redelivery and late-arriving older txns), durable DLQ fence on apply
failure, fencing-token lease for clean failover. Scales within a single
Fargate task via per-key stripe-locked parallel apply + micro-batching
(128 workers / 8192 stripes / BATCH_MAX_TXNS=2 at the current shape) and
multi-process children (`REPLICATOR_PROCESSES=2`) for GIL relief.

---

## Reading order

0. **`docs/design-tour.html`** — interactive single-page tour of the design
   with step-through scenarios for each of the correctness-critical paths
   (atomic checkpoint, cross-shard reassembly, LWW race, ingest-DLQ, apply
   failure + fence). Open the file directly, or view online via
   [raw.githack](https://rawcdn.githack.com/aws-samples/sample-aurora-dsql-multi-region-async-read-replicas/main/docs/design-tour.html).
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
│ │ ├── cli.py ← click entry points: migrate | consume | ops | healthz
│ │ ├── config.py ← env-driven Config dataclass
│ │ ├── envelope.py ← CDC envelope parser
│ │ ├── reassembler/ ← fragment + transaction reassembly
│ │ ├── ordering/ ← priority queue + per-shard watermark + fence gate
│ │ ├── applier/ ← stripe-locked parallel apply + schema cache
│ │ ├── dlq/ ← fence writer + payload (de)serialize
│ │ ├── state/ ← checkpoints + shard leases + watermark state
│ │ ├── consumer/ ← end-to-end pipeline + metrics loop + liveness
│ │ ├── migrate/ ← CloudWatch dashboard upsert
│ │ ├── ops/ ← operator CLI subcommands
│ │ ├── metrics.py ← CloudWatch emitter
│ │ └── health.py ← healthz probes (DSQL + SNS + liveness file)
│ └── tests/ ← pytest unit tests
│
├── infra/ ← AWS CDK Python deployment
│ ├── app.py
│ ├── cdk.json
│ ├── requirements.txt
│ ├── README.md
│ └── stacks/
│ └── replicator_stack.py ← single-stack: VPC + endpoints + NAT + ECR asset +
│ KMS + 2 SNS topics + 5 alarms + Fargate task + service
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

See [`infra/README.md`](./infra/README.md) for prereqs and the end-to-end
deploy / destroy steps.

```bash
cd infra/
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

AWS_REGION=<control-region> .venv/bin/cdk bootstrap
```

Prereq: Docker Desktop running (CDK asset builds the image locally).

The replicator always needs the **target** cluster, plus one of two ways to
identify the **source** — pick the mode that matches what you already have.

### Mode A — You know the source cluster id

The preferred mode when the source DSQL cluster already exists and has CDC
enabled. On startup the replicator calls `dsql:ListStreams` + `dsql:GetStream`
on the source cluster, picks the ACTIVE stream, and reads the Kinesis stream
ARN out of its CDC configuration — you never type a stream name:

```bash
.venv/bin/cdk deploy \
 -c source_cluster_id=<SOURCE_CLUSTER_ID> \
 -c source_region=<source-region> \
 -c target_cluster_id=<TARGET_CLUSTER_ID> \
 -c target_region=<target-region>
```

If no ACTIVE stream is attached, the preflight fails fast with a clear message
instead of silently consuming from nothing.

### Mode B — You know the Kinesis stream directly

Use this when CDC was wired up outside this project and you already have the
stream name — e.g. a shared ingest stream, or a demo scenario where the
loadtest tooling publishes synthetic records directly to Kinesis:

```bash
.venv/bin/cdk deploy \
 -c source_stream_name=<KINESIS_STREAM_NAME> \
 -c source_region=<source-region> \
 -c target_cluster_id=<TARGET_CLUSTER_ID> \
 -c target_region=<target-region>
```

`source_stream_name` defaults to `dsql-cdc-stream`, so this collapses to
`-c target_cluster_id=<...> -c target_region=<...>` in the demo scenario.

### Why not infer the source from the target alone?

DSQL has no back-reference from a target cluster to "the cluster whose CDC I'm
consuming." The two clusters are independent — the replicator is what links
them. So the operator always names the source somehow: either the **cluster
id** (Mode A, strongly preferred for production) or the **Kinesis stream**
(Mode B, for cases where the cluster id is not available).

---

## Known limits

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
