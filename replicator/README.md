# dsql-replicator

Aurora DSQL multi-region async replicator. Reads CDC from Kinesis, applies transactions
atomically to a target DSQL cluster in another region.

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
dsql-replicator migrate                    # idempotently upsert the CloudWatch dashboard
dsql-replicator consume                    # run the consumer / applier pipeline
dsql-replicator healthz                    # container healthcheck: DSQL + SNS + liveness
dsql-replicator ops dlq list|inspect|retry|skip|edit-and-retry
dsql-replicator ops pause|unpause
```

All config via environment variables; see `src/dsql_replicator/config.py` for the full list.

## Test / lint / type-check

```
ruff check . && ruff format --check .
mypy
pytest -q                                  # 108 unit tests
```

## Deployment

The replicator ships as a container to AWS Fargate via a CDK Python app in `../infra/`.
See `../infra/README.md` for prereqs + deploy steps and `../docs/RUNBOOK.md` for
on-call ops.
