# dsql-replicator — Architecture

**Last updated:** 2026-11 (post-simplification pass)

This document shows the pieces + how they talk. Row-level LWW is the current contract; the former cross-shard transaction reassembly, ordering buffer, and DLQ fence were removed in Nov 2026. See `DESIGN.md §1a` for the current correctness contract.

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
                        │ dsql CDC         │   ┌──────────────────────────────────────┐
                        │ (IAM role,       │   │ __replicator_lww_stamps              │
                        │  partitionKey    │   │   (schema, table, pk, applied_ts_ns) │
                        │  = tx_id)        │   │ __replicator_checkpoints             │
                        └──────────────────┘   │ __replicator_shard_leases            │
                                               └──────────────────────────────────────┘

                                   control-plane services (us-east-1):
                                   SNS dsql-replicator-alarms       (3 CW alarms)
                                   CW  /aws/ecs/dsql-replicator     (logs, metrics, dashboard)
```

---

## 2. Inside the Fargate task

```
Kinesis GetRecords (per shard)
    │
    │  (SequenceNumber, Data bytes)
    ▼
envelope.parse  →  FragmentReassembler
    (pydantic)     (chunked CRC32C — >1 MB rows)
    │
    │  ReadyRecord (FullRecord | AssembledRecord)
    ▼
asyncio.Queue  (apply_queue, bounded by ORDERING_BUFFER_MAX_DEPTH)
    │
    │   _QueueItem(record, shard_id, seq_str)
    ▼
_apply_worker × N          (N = applier_workers)
    │
    │  micro-batch up to BATCH_MAX_TXNS or BATCH_MAX_WAIT_MS
    ▼
Applier.apply_batch(records, pre_commit_hook=checkpoint_hook)
    │
    │  1. compute touched stripes
    │     (hash of (schema, table, pk-tuple))
    │  2. acquire stripe locks in sorted order
    │     (AsyncExitStack, deadlock-free)
    │  3. borrow pool connection (recycle if token age > 12 min)
    │  4. BEGIN
    │       for each record:
    │         INSERT stamp ON CONFLICT DO UPDATE
    │           WHERE existing.applied_ts_ns < EXCLUDED.applied_ts_ns
    │         if stamp advanced:  UPSERT / DELETE row
    │       pre_commit_hook(cur):  advance __replicator_checkpoints
    │                              for every contributing shard
    │     COMMIT  (if transient failure: ROLLBACK + infinite retry
    │              with exponential backoff capped at 10s)
    │  5. return connection, release stripes
    ▼
Target DSQL (us-west-2)
```

Side tasks (one per pipeline):
- `_metrics_loop` — PutMetricData every `METRICS_INTERVAL_MS` (counters + gauges + per-shard metrics).
- `_reassembly_sweeper` — process-global fragment timeout sweep (fragments can arrive on different shards).
- `_checkpoint_flush_loop` — periodic best-effort flush of per-shard checkpoints for shards whose records are parked in fragment reassembly.
- `_heartbeat_loop` (one per shard worker) — UPDATE `__replicator_shard_leases` with fencing-token semantics.
- `_shard_discovery_loop` — re-list Kinesis shards every `SHARD_DISCOVERY_INTERVAL_MS`.
- Writer autoscaler — grow / shrink the apply pool between `APPLIER_WORKERS` and `APPLIER_WORKERS_MAX` based on sustained `ReplicationLagMs`.

---

## 3. Happy-path sequence (one record)

```
Source  ─ COMMIT ─►  DSQL CDC  ─ Put ─►  Kinesis  ─ GetRecords ─►  Fargate
                                            │
                                 shard worker:
                                   parse(data)
                                   fragment.ingest(env)       → None | AssembledRecord | FullRecord
                                   apply_queue.put(_QueueItem(ready, shard_id, seq))
                                   (loop; shard discovery + heartbeat run in parallel)

                                 apply worker:
                                   batch = drain queue up to BATCH_MAX_TXNS
                                           or BATCH_MAX_WAIT_MS
                                   Applier.apply_batch(records, pre_commit_hook=cp)
                                     compute_touched_stripes(records)   → {s1, s2, …}
                                     async with stripes[sorted]:
                                       conn = await pool.get()
                                       async with conn.transaction():
                                         for rec in records:
                                           stamp = INSERT ON CONFLICT WHERE older_ts_ns
                                           if stamped: UPSERT / DELETE row
                                         pre_commit_hook(cur):
                                           advance checkpoints for contributing shards
                                       pool.put(conn)
                                     emit apply.ok
                                                                                ↓
                                                                            Target DSQL
                                                                            (row + stamp + checkpoint committed atomically)
```

Transient failures (OperationalError, SerializationFailure, deadlock, timeout, connection reset) roll the target transaction back and retry forever with exponential backoff. `ApplyError` (schema missing, malformed record) is logged + the batch dropped so other keys keep flowing; checkpoint still advances so forward progress isn't permanently stuck.

---

## 4. Lease failover

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

                                                                               Resume applying from checkpoint;
                                                                               __replicator_lww_stamps per-key
                                                                                 ts_ns guard skips any replay
```

Fencing token: if task A later wakes up and tries to heartbeat with its CACHED lease_token, the UPDATE matches 0 rows (lease_token mismatch) → task A gracefully shuts down the shard worker.

---

## 5. Correctness invariants (current)

| Invariant | Guarantee |
|---|---|
| **Per-row atomic apply** | ONE DSQL transaction per batch: all `(row DML + stamp advance + per-shard checkpoint upserts)` commit together or none. |
| **Per-key last-writer-wins** | `__replicator_lww_stamps(schema, table, pk, applied_ts_ns)` stamp. UPSERT inside the same target txn as the row apply, gated on `existing.applied_ts_ns < EXCLUDED.applied_ts_ns`. Absorbs at-least-once CDC redelivery and late-arriving older commits; convergence per primary key is guaranteed. |
| **Per-key commit order** | Records touching the same primary key apply in source `ts_ns` order (stripe lock serializes them; LWW gate enforces order). |
| **Cross-key / cross-row order** | **NOT preserved.** Two source commits on different rows can apply on the replica in any order — see `DESIGN.md §1a` (coffee / dinner example). |
| **Checkpoint resumability** | `__replicator_checkpoints` is advanced in-band with apply for contributing shards + periodic best-effort flush for quiet shards. Restart recovers via `AFTER_SEQUENCE_NUMBER` iterator. Replay dedupes via the LWW stamp. |
| **Fragment loss window** | A record whose fragments were parked in `FragmentReassembler` at crash time is lost. LWW catches up on the next source write to that primary key. |

---

## 6. State plane shape

All state in target DSQL. **Three tables**, bootstrapped idempotently by `Applier.start()` on every task startup:

```
__replicator_lww_stamps     (schema, table, pk)         — per-primary-key LWW stamp carrying latest applied_ts_ns
__replicator_checkpoints    (stream, shard)             — Kinesis seq + ts_ns per shard
__replicator_shard_leases   (stream, shard)             — single-writer lease + fencing token
```

The former `__replicator_applied_v2`, `__replicator_late_discards`, `__replicator_dlq_fences`, `__replicator_dlq_payload`, `__replicator_dlq_ingest`, and `__replicator_watermark_state` tables were removed in the 2026-11 simplification pass; operators upgrading from an earlier version can drop them manually (not required — they're inert and small).

---

## 7. What's NOT in this architecture

- **Transaction reassembly across shards.** Removed in 2026-11 — Kinesis cannot preserve DSQL transaction order across shards, so cross-row atomicity is not provided on the replica. See `DESIGN.md §1a`.
- **DLQ fence / quarantine.** Removed. Transient apply failures retry forever; persistent failures are logged + dropped so other keys keep flowing.
- **Ops CLI (`ops dlq ...`, `pause`/`unpause`).** Removed with the DLQ.
- **Multi-task scale.** `desiredCount=1` enforced. The multi-process supervisor (`REPLICATOR_PROCESSES`) runs N children inside ONE task for GIL relief; it does not scale beyond a single Fargate task.
- **Multi-shard Kinesis.** The default stream is single-shard. Multi-shard producer-side partitioning is a separate design.
- **Multi-target per process.** One target DSQL cluster per task.
- **Admin HTTPS API.** CLI only (`migrate`, `consume`, `healthz`).
- **Cross-stream ordering.** Each `ConsumerPipeline` instance runs independently; cross-stream order not enforced.
