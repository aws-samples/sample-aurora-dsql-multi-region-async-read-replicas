# Aurora DSQL Multi-Region Async Replicator

A Python + CDK + Fargate reference implementation that reads Aurora DSQL CDC
from Kinesis in one region and applies it atomically to a target DSQL cluster
in another region.

Correctness-first: per-transaction reassembly, per-shard ordering, idempotent
apply via per-transaction stamps in the target cluster, durable DLQ fence on
apply failure, fencing-token lease for clean failover. Scales horizontally
within a single task via per-key stripe-locked parallel apply (256 workers /
512 stripes at the current shape).

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
| **Micro-batching + multi-proc (2026-10-26)** | 128 or 256 / 512 / 8 | **~88** | **~310** |

Source side load-tested at 530 TPS for 30 min against the dsql cluster
`kbud4lp76tyvoj5zligoi5otlm.dsql.us-east-1.on.aws` with zero errors, zero
fences, zero credential rotations lost; the credential-rotation fix (commit
`6286c6f`) was validated in the same run.

### The 2026-10-26 ceiling

The micro-batching + multi-process build (commits `6187127`, `89a4907`,
`5c07676`) measured a **~88 TPS applier ceiling that did not move** when
`APPLIER_WORKERS` was raised from 128 → 256 or when `BATCH_MAX_TXNS` was
tuned 1 → 16. Target Fargate CPU stayed below 20% of one vCPU; the pool had
118+ idle connections at all times; `stripes_count` per batch was ~2 (vs.
~50 under `BATCH_MAX_TXNS=16` which previously masked the limit as apparent
stripe-lock contention). The ceiling appears to be a target-DSQL
concurrent-write cap enforced server-side rather than any client-side
knob. See `docs/DESIGN.md` for the measurement writeup and open hypotheses.

---

## Known limits

- **Target-side concurrent-write ceiling (~88 TPS/cluster)**: the dominant
  limit on this build. Not related to connection count, pool size, workers,
  stripes, or batching. Investigation open (see `docs/DESIGN.md`).
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
