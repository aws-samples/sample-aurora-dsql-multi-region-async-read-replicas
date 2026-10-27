# dsql-replicator — Architecture

**Last updated:** 2026-10-12 (through M6.0)

This document shows the pieces + how they talk.

---

## 1. Component map

```
                              us-east-1                                 us-west-2
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  Source DSQL     │    │  Kinesis Stream  │    │  Fargate Task    │    │  Target DSQL     │
│  (OLTP writes)   │───►│  dsql-cdc-stream │───►│  dsql-replicator │───►│  (replicated)    │
└──────────────────┘    │  (24h retention) │    │ stripes=8192 w=128│    └──────────────────┘
                        └──────────────────┘    └──────────────────┘             ▲
                                 ▲                       │                       │
                                 │                       ├────── state plane ────┤
                                 │                       │  (bootstrapped on     │
                                 │ CDC emission          │   Applier.start)      │
                                 │                       │                       │
                        ┌──────────────────┐             ▼                       ▼
                        │ dsql CDC   │   ┌──────────────────────────────────────┐
                        │ (IAM role,       │   │ __replicator_applied_v2 (schema,table,pk,ts_ns) │
                        │  partitionKey    │   │ __replicator_dlq_fences              │
                        │  = tx_id)        │   │ __replicator_dlq_payload             │
                        └──────────────────┘   │ __replicator_checkpoints             │
                                               │ __replicator_shard_leases            │
                                               │ __replicator_watermark_state         │
                                               └──────────────────────────────────────┘

                                   control-plane services (us-east-1):
                                   SNS dsql-replicator-dlq-events   (fence open/closed/skipped)
                                   SNS dsql-replicator-alarms       (5 CW alarms)
                                   CW  /aws/ecs/dsql-replicator     (logs, metrics, dashboard)
```

---

## 2. Inside the Fargate task

```
Kinesis GetRecords (per shard)
    │
    │  (SequenceNumber, Data bytes)
    ▼
envelope.parse  →  FragmentReassembler  →  TransactionReassembler
    (pydantic)     (chunked CRC32C)         (buffer by txId, emit at total_records)
    │
    │  CompletedTransaction
    ▼
OrderingBuffer         (heap by source.ts_ns, release < watermark)
    │                   (fence gate — M3)
    │
    │  ready() list
    ▼
_apply_runner  —  Semaphore(worker_count=32)  —  asyncio.create_task(_apply_one)
    │                                                   │
    │                                                   ▼
    │                                     Applier.apply(txn)
    │                                       │
    │                                       │  1. compute touched stripes
    │                                       │     (hash of (schema, table, pk-tuple))
    │                                       │  2. acquire stripe locks in sorted order
    │                                       │     (AsyncExitStack, deadlock-free)
    │                                       │  3. borrow connection from pool (32 conns)
    │                                       │  4. BEGIN
    │                                       │       for each record:
    │                                       │         INSERT stamp (if new)
    │                                       │         UPSERT / DELETE row
    │                                       │       pre_commit_hook(cur)  — if ops dlq retry
    │                                       │     COMMIT
    │                                       │  5. return connection, release stripes
    │                                       ▼
    │                                 Target DSQL (us-west-2)
    │
    │  on ApplyError → open fence  (DlqWriter.open_fence → 2 target-DSQL rows + SNS)
    ▼
    (next iteration)
```

Side tasks (one per pipeline):
- `_fence_poller` — SELECT on `__replicator_dlq_fences` every `FENCE_POLL_INTERVAL_MS`; clears in-memory fence on CLOSED.
- `_metrics_loop` — PutMetricData every `METRICS_INTERVAL_MS` (counters + gauges + per-shard metrics).
- `_heartbeat_loop` (one per shard worker) — UPDATE `__replicator_shard_leases` with fencing-token semantics.

---

## 3. Happy-path sequence (one txn commit)

```
Source  ─ COMMIT ─►  DSQL CDC  ─ Put ─►  Kinesis  ─ GetRecords ─►  Fargate
                                            │
                                 shard worker:
                                   parse(data)
                                   fragment.ingest(env)       → None | AssembledRecord | FullRecord
                                   txn.ingest(ready)          → None | CompletedTransaction
                                   ordering.ingest(completed)
                                   ordering.update_shard(watermark, mbl)
                                   checkpoint flush (every CHECKPOINT_FLUSH_INTERVAL_MS)

                                 apply runner:
                                   ready = ordering.ready()
                                   for completed in ready:
                                     Semaphore acquire
                                     create_task(_apply_one(completed))

                                 _apply_one:
                                   compute_touched_stripes(txn)         → {s1, s2, …}
                                   async with stripes[sorted]:
                                     conn = await pool.get()
                                     async with conn.transaction():
                                       stamp + upsert/delete per record
                                     pool.put(conn)
                                   emit apply.ok
                                                                                ↓
                                                                            Target DSQL
                                                                            (row applied + stamp written atomically)
```

---

## 4. DLQ fence lifecycle (M3)

```
             permanent apply failure
                   │
                   ▼
Applier raises ApplyError
                   │
                   ▼
pipeline._apply_one sets `fenced` event
                   │
                   ▼
pipeline._open_fence:
   DlqWriter.open_fence(txn, error_class, failing_sql)
      target DSQL:  INSERT fence row + payload row (ONE TXN)
      SNS:          publish `opened` to dsql-replicator-dlq-events
   OrderingBuffer.set_fence(failed.ts_ns)
                   │
                   ▼
pipeline keeps buffering new records; applier commits nothing past fence_ts_ns
                   │
                   │  (fence poller sees OPEN row every 5s;
                   │   in-memory fence stays set)
                   ▼
operator runs `ops dlq retry <tx_id>`:
   loads payload from target DSQL
   deserialize_txn → CompletedTransaction
   Applier.apply(txn, pre_commit_hook=close_fence_in_cursor)
      inside ONE DSQL txn: insert rows + stamps + UPDATE fence status=CLOSED
   SNS: publish `closed`
                   │
                   ▼
fence poller sees no OPEN rows → OrderingBuffer.set_fence(None)
   buffered txns drain in per-key order
```

---

## 5. Lease failover (M4)

```
         task A healthy                 task A crashes          task B spawns (ECS)
───────────────────────────────────────────────────────────────────────────────────►
heartbeat every             (no heartbeat)          lease expires              startup:
10s updates expires_at                              in __replicator_shard_leases
                                                                               LeaseRepo.steal:
                                                                                 UPDATE … WHERE
                                                                                   (task_id=self OR expires_at<now())
                                                                                 RETURNING lease_token

                                                                               CheckpointRepo.get:
                                                                                 SELECT sequence_number
                                                                               → iterator = AFTER_SEQUENCE_NUMBER

                                                                               Resume applying from checkpoint
                                                                               __replicator_applied_v2 per-key
                                                                                 ts_ns guard skips any replay
```

Fencing token: if task A later wakes up and tries to heartbeat with its CACHED lease_token, the UPDATE matches 0 rows (lease_token mismatch) → task A gracefully shuts down the shard worker.

---

## 6. Ordering invariants (post-M6.0)

| Invariant | Guarantee |
|---|---|
| **I0** — Per-row atomic apply | ONE DSQL transaction per CompletedTransaction. All rows + stamps commit together or none. |
| **I2** — Per-key last-writer-wins (idempotent) | `__replicator_applied_v2(schema, table, pk, applied_ts_ns)` stamp table. UPSERT inside the same target txn as the row apply, gated on `existing.applied_ts_ns < EXCLUDED.applied_ts_ns`. Absorbs at-least-once CDC redelivery and late-arriving older txns; convergence per key is guaranteed. |
| **I3** — Per-shard watermark | OrderingBuffer tracks each shard's `latest_ts_ns`; `min()` across shards gates release. Idle shards synthetically advance only when `MillisBehindLatest = 0`. |
| **I5** — Checkpoint resumability | `__replicator_checkpoints` upserted periodically; restart recovers via `AFTER_SEQUENCE_NUMBER` iterator. Replay dedupes via I2. |
| **I6** — Intra-transaction order | TransactionReassembler emits records sorted by `record_id`. Applier applies in that order inside the one DSQL txn. |
| **I8'** — Per-key commit order (post-M6.0) | Txns touching the same key commit in source `ts_ns` order via stripe locks. **Cross-key order is NOT preserved** — standard CDC behavior. |

Reversed vs the original M0 spec:
- **I8** (global `ts_ns` commit order) → **I8'** (per-key).

---

## 7. State plane shape

All state in target DSQL.

```
__replicator_applied_v2       (schema,table,pk)        — per-key LWW stamp carrying latest applied_ts_ns (post-2026-10-26)
__replicator_late_discards    (tx_id, record_id)       — audit of rows the LWW guard rejected (older t_commit arrived late)
__replicator_dlq_fences       (tx_id)                  — fence rows, status OPEN/CLOSED/SKIPPED (M3)
__replicator_dlq_payload      (tx_id)                  — full CompletedTransaction JSONB (M3)
__replicator_checkpoints      (stream, shard)          — Kinesis seq + ts_ns per shard (M4)
__replicator_shard_leases     (stream, shard)          — single-writer lease + fencing token (M4)
__replicator_watermark_state  (stream, shard)          — persisted mirror for cold restart (M4; unused yet)
__replicator_applied          (tx_id, record_id)       — DEPRECATED, left in place from pre-2026-10-26; may be dropped
```

Bootstrapped idempotently by `Applier.start()` on every task startup.

---

## 8. What's NOT in this architecture

- **Multi-task scale.** `desiredCount=1` enforced. The multi-process supervisor
  (`REPLICATOR_PROCESSES`) runs N children inside ONE task for GIL relief; it
  does not scale beyond a single Fargate task. Scaling to >1 task requires
  deterministic shard-task partition (open TODO).
- **Multi-shard Kinesis.** Single-shard stream by default. Multi-shard producer-side partitioning is a separate design.
- **Multi-target per process.** One target DSQL cluster per task.
- **Admin HTTPS API.** CLI only.
- **KMS CMK on Kinesis/SNS/DSQL.** AWS-managed keys currently. Prod cutover would add CMKs.
- **Cross-stream ordering.** Each `ConsumerPipeline` instance runs independently; cross-stream order not enforced.
