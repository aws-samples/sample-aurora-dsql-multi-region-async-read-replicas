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

1. **`docs/OVERVIEW.md`** — code-free architecture walkthrough. Start here.
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
│ ├── OVERVIEW.md ← code-free architecture (start here)
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
├── load-test/
│ └── generate_load.py ← async source-side load generator with ramp-up
│
└── dsql-cdc-tail.py ← convenience: tail a CDC stream in your terminal
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
deploy / destroy steps. Short version:

```bash
cd infra/
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

AWS_REGION=<control-region> .venv/bin/cdk bootstrap
.venv/bin/cdk deploy \
 -c target_cluster_id=<TARGET_CLUSTER_ID> \
 -c target_region=<target-region> \
 -c target_endpoint_url=https://dsql.<target-region>.api.aws
```

Prereq: Docker Desktop running (CDK asset builds the image locally).

---

## Measured throughput

Cross-region, zero apply errors, no fences opened.

| Shape | Workers / Stripes / vCPU | Target TPS | DMLs/sec |
|---|---|---|---|
| Serial lane (baseline) | 32 / 64 / 1 | ~8 | ~27 |
| Vertical scale | 128 / 256 / 2 | ~24 | ~85 |
| Vertical push | 256 / 512 / 4 | ~250 | ~870 |
| Early 2026-10-26 (stripes=512) | 128 / 512 / 8 | ~88 | ~310 |
| **2026-10-26 after stripe fix** | **128 / 8192 / 8** | **~700** | **~2,400** |

Source side load-tested at 533 TPS for 7 min against the dsql cluster
`<SOURCE_CLUSTER_ID>.dsql.us-east-1.on.aws` with zero errors, zero
fences, zero credential rotations lost. The credential-rotation fix
(commit `6286c6f`) was validated across both the 30-min and the 7-min
runs. Applier TPS (~700) exceeded the source TPS (~533) and drained
the ordering buffer backlog from the earlier 30-min run.

### Earlier "88 TPS cap" — a client-side regression, not a server limit

Our 09:xx run claimed a target-DSQL concurrent-write cap. **That was
wrong.** Per-stage instrumentation (commit `29850a3`) showed the real
cost was `asyncio.Lock` contention on `APPLIER_STRIPES`. With 128
workers each acquiring ~7 of only 512 stripes, collision probability
was ~99 % per queue-position, and `stripe_ms` was 24+ seconds at p95
despite healthy commit times. Raising `APPLIER_STRIPES` to 8192 dropped
`stripe_ms` to near zero and lifted the applier from 88 → ~700 TPS.
Full writeup in `docs/DESIGN.md`.

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
