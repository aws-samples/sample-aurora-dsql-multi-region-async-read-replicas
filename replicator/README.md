# dsql-replicator

Aurora DSQL multi-region async replicator. Reads CDC from Kinesis and applies rows
to a target DSQL cluster in another region with **row-level eventual consistency** —
each record upserts the row and bumps a `(schema, table, pk, applied_ts_ns)` stamp
in `__replicator_lww_stamps`, gated by last-writer-wins on `applied_ts_ns`. Rows
converge per primary key; cross-row transactional invariants are not preserved on
the replica. See the top-level `../README.md` "Row-level eventual consistency"
section and `../docs/DESIGN.md` §1a for the full contract.

See `../docs/OVERVIEW.md` for a code-free architecture walkthrough and
`../docs/ARCHITECTURE.md` for component diagrams and sequence flows.

## Install (dev)

Python 3.12+:

```
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
```

## Commands

```
dsql-replicator migrate     # idempotently upsert the CloudWatch dashboard
dsql-replicator consume     # run the consumer / applier pipeline
dsql-replicator healthz     # container healthcheck: DSQL + liveness file
```

All config via environment variables; see `src/dsql_replicator/config.py` for the full list.

## Test / lint / type-check

```
ruff check . && ruff format --check .
mypy
pytest -q
```

## Deployment

The replicator ships as a container to AWS Fargate via a CDK Python app in `../infra/`.
See `../infra/README.md` for prereqs + deploy steps and `../docs/RUNBOOK.md` for
on-call ops.
