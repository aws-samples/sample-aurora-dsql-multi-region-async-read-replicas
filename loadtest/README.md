# loadtest/

Scripts for exercising the replicator end-to-end. Three source-side patterns live here, each matching a different test scenario; pick the one whose source model matches what you are trying to prove.

## Pattern 1 — direct Kinesis publisher (fastest to set up)

Publishes well-formed CDC envelopes straight into a Kinesis stream, bypassing any upstream database. Use when you want to:

- Load-test the replicator at a target TPS without standing up a source database.
- Inject malformed records, bad-CRC fragments, cross-shard chunked txns, or older-ts replays in controlled proportions.
- Verify dashboard metrics, DLQ-ingest routing, LWW audit, and cross-shard reassembly.

| Script | Role |
|---|---|
| `synthetic_publisher.py` | Pumps `full` / `chunked` / `fragment` records into Kinesis at a ramped TPS. Flags cover injection ratios for parse errors, bad CRC, LWW replays, chunked fraction. |
| `_setup_demo_schema.py` | Creates the `loadtest_events` table on the target DSQL cluster the publisher writes to. |

```bash
# 1. Target DSQL has `loadtest_events` ready.
TARGET_CLUSTER_ID=<cluster> TARGET_REGION=us-east-2 \
  python _setup_demo_schema.py

# 2. Publish for 5 min ramp + 30 min hold at 50 TPS.
python synthetic_publisher.py \
  --stream-name <kinesis-stream> --region us-east-2 \
  --target-tps 50 --ramp-start-tps 5 --ramp-s 300 --hold-s 1800 \
  --chunked-ratio 0.10 --bad-parse-ratio 0.005 --bad-crc-ratio 0.005 --lww-replay-ratio 0.02
```

## Pattern 2 — DSQL-source async load generator

Writes directly to a source DSQL cluster at a steady-rate TPS. Use when you want to exercise the full source-cluster → DSQL CDC → Kinesis → replicator pipeline.

| Script | Role |
|---|---|
| `generate_load.py` | Steady-rate DSQL load generator with ramp-up; emits per-minute throughput and p50/p95/p99 commit latency. |
| `schemas.sql` | DDL used by the generator. |

## Pattern 3 — Aurora PostgreSQL source loaders

Scripts that originated with the earlier Postgres-source test harness (Aurora PG → DMS → Kinesis → replicator → target DSQL). Use when the test is specifically about the Postgres-origin path.

| Script | Role |
|---|---|
| `pg_tps.py` | Steady-rate autocommitted INSERT/UPDATE/DELETE mix against Aurora PG. One SQL txn per op → one CDC event per op. |
| `pg_batch_load.py` | Batched variant (N DMLs per txn); amortizes commit cost. |
| `pg_customers_load.py` | One-shot customer-table backfill. |
| `pg_load_1m.sql` | Seeds ~1 M rows per table on the source. |
| `pg_append_5m.sql` | Appends another 5 M rows after seeding. |

All three Python scripts take `--host <your-aurora-pg-endpoint>` and read `PGPASSWORD` from env.

## Utility

| Script | Role |
|---|---|
| `dsql-cdc-tail.py` | Convenience: tail a DSQL CDC Kinesis stream in your terminal so you can see envelopes as they land. Not part of load generation; useful for debugging. |
