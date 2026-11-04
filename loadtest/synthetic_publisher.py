"""Synthetic DSQL CDC envelope publisher for load-testing the replicator.

Pumps `full`, `chunked`, and `fragment` records at a target TPS into a Kinesis
stream, exercising every code path the reviewer flagged:

- **Cross-shard reassembly (#4)**: chunked records' fragments are published
  with partition keys that spread them across all stream shards. If the
  reassembler regresses to per-shard, these transactions never apply.
- **Ingest-side DLQ routing (#2)**:
    - a configurable `--bad-parse-ratio` injects malformed bytes.
    - a configurable `--bad-crc-ratio` publishes fragments whose claimed
      CRC32C doesn't match the body.
- **Atomic checkpoint (#1)**: the applier commits each txn with the
  pre_commit_hook upserting `__replicator_checkpoints`. On the dashboard,
  `CheckpointsFlushed` should grow in step with `TransactionsCommitted`.
- **LWW framing (#5)**: a fraction of records intentionally re-send an older
  `ts_ns` for a key already applied. These should land in the
  `RowsLateDiscarded` metric, not `RowsReplicated`.
- **Fence operational pause (#6)**: not exercised here — would need an apply
  failure, which the operator triggers via `ops dlq fail-next`. Out of scope
  for this run.

Ramp model: linear from `--ramp-start-tps` to `--target-tps` over `--ramp-s`
seconds, hold at `--target-tps` for `--hold-s` seconds, then stop. Logs a
one-line status every 10 s.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import time
import uuid

import aioboto3
from crc32c import crc32c as _crc32c


def _ts_fields(ts_ns: int) -> dict[str, int]:
    return {"ts_ms": ts_ns // 1_000_000, "ts_ns": ts_ns}


def _source(tx_id: str, ts_ns: int) -> dict:
    return {
        "version": "1.1",
        "ts_ms": ts_ns // 1_000_000,
        "ts_ns": ts_ns,
        "txId": tx_id,
        "schema": "public",
        "table": "loadtest_events",
        "db": "postgres",
        "cluster": "loadtest",
    }


def _full_envelope(event_id: str, tenant: str, payload: str, ts_ns: int, tx_id: str, record_id: str) -> dict:
    return {
        "type": "full",
        "op": "c",
        "before": None,
        "after": {"event_id": event_id, "tenant": tenant, "payload": payload, "updated_ts": ts_ns},
        "source": _source(tx_id, ts_ns),
        **_ts_fields(ts_ns),
        "transaction": {"total_records": 1, "record_id": record_id},
    }


def _chunked_main(
    event_id: str, chunk_id: str, total_fragments: int, crc: str, ts_ns: int, tx_id: str, record_id: str
) -> dict:
    return {
        "type": "chunked",
        "op": "c",
        "before": None,
        "after": None,
        "source": _source(tx_id, ts_ns),
        "chunked": {"after": {"chunk_id": chunk_id, "total_fragments": total_fragments, "crc32c": crc}},
        **_ts_fields(ts_ns),
        "transaction": {"total_records": 1, "record_id": record_id},
    }


def _fragment(chunk_id: str, index: int, data: str) -> dict:
    return {"type": "fragment", "chunk_id": chunk_id, "index": index, "data": data}


def _split(s: str, n: int) -> list[str]:
    step = max(1, len(s) // n)
    out = [s[i : i + step] for i in range(0, len(s), step)]
    while len(out) < n:
        out.append("")
    if len(out) > n:
        out[n - 1] = "".join(out[n - 1 :])
        out = out[:n]
    assert "".join(out) == s
    return out


def _rand_payload(nbytes: int = 80) -> str:
    return "x" * nbytes


async def _put_records(client, stream_name: str, records: list[tuple[str, dict]]) -> None:
    """One PutRecords call for a batch of (partition_key, envelope) pairs."""
    entries = [
        {"Data": json.dumps(env).encode("utf-8"), "PartitionKey": pk}
        for pk, env in records
    ]
    # PutRecords max 500; we cap at 250 per call to keep failures manageable.
    for i in range(0, len(entries), 250):
        chunk = entries[i : i + 250]
        resp = await client.put_records(StreamName=stream_name, Records=chunk)
        failed = int(resp.get("FailedRecordCount", 0))
        if failed:
            # Retry-once; otherwise accept the loss for a load test.
            retry = [
                chunk[j]
                for j, r in enumerate(resp.get("Records", []))
                if "ErrorCode" in r
            ]
            if retry:
                await asyncio.sleep(0.1)
                await client.put_records(StreamName=stream_name, Records=retry)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream-name", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--target-tps", type=float, default=50.0)
    ap.add_argument("--ramp-start-tps", type=float, default=1.0)
    ap.add_argument("--ramp-s", type=int, default=300, help="ramp duration seconds")
    ap.add_argument("--hold-s", type=int, default=1800, help="steady-state duration seconds")
    ap.add_argument(
        "--chunked-ratio",
        type=float,
        default=0.10,
        help="fraction of txns emitted as chunked (fragments spread across shards)",
    )
    ap.add_argument(
        "--bad-parse-ratio", type=float, default=0.005, help="fraction of records with malformed JSON"
    )
    ap.add_argument(
        "--bad-crc-ratio",
        type=float,
        default=0.005,
        help="fraction of chunked records with a corrupted CRC32C",
    )
    ap.add_argument(
        "--lww-replay-ratio",
        type=float,
        default=0.02,
        help="fraction of txns that re-send an older ts for an existing key (LWW audit)",
    )
    ap.add_argument(
        "--num-keys", type=int, default=5_000, help="distinct event_id pool size (for LWW replay)"
    )
    ap.add_argument("--tenants", type=int, default=50, help="distinct tenant pool size")
    args = ap.parse_args()

    key_pool = [f"evt-{i:08x}" for i in range(args.num_keys)]
    tenants = [f"tenant-{i:03d}" for i in range(args.tenants)]
    # Remember the last ts_ns we sent per key so we can emit a stale one for LWW.
    last_ts_by_key: dict[str, int] = {}

    session = aioboto3.Session()
    async with session.client("kinesis", region_name=args.region) as kin:
        t0 = time.monotonic()
        total_published = 0
        total_parse_bad = 0
        total_crc_bad = 0
        total_chunked = 0
        total_lww_replay = 0
        last_report = t0

        print(
            f"[{int(time.time())}] starting publisher: ramp {args.ramp_start_tps}->{args.target_tps} "
            f"over {args.ramp_s}s, hold {args.hold_s}s"
        )

        while True:
            now = time.monotonic()
            elapsed = now - t0
            total_s = args.ramp_s + args.hold_s
            if elapsed > total_s:
                break
            # Current target TPS
            if elapsed < args.ramp_s:
                frac = elapsed / args.ramp_s
                cur_tps = args.ramp_start_tps + (args.target_tps - args.ramp_start_tps) * frac
            else:
                cur_tps = args.target_tps

            # Build a 500 ms batch worth of records.
            batch_window_s = 0.5
            n = max(1, int(cur_tps * batch_window_s))
            records: list[tuple[str, dict]] = []
            now_ns = time.time_ns()
            for _ in range(n):
                tx_id = uuid.uuid4().hex
                record_id = "r0"
                tenant = random.choice(tenants)
                payload = _rand_payload()
                roll = random.random()

                if roll < args.bad_parse_ratio:
                    # Garbage bytes — exercises parse_error path.
                    records.append((f"bad-{tx_id}", {"__raw__": "}}}not json{{{"}))
                    total_parse_bad += 1
                    continue

                # 2% chance: re-send a stale (lower-ts) record for a key we already saw.
                if last_ts_by_key and random.random() < args.lww_replay_ratio:
                    key = random.choice(list(last_ts_by_key.keys()))
                    older_ts = last_ts_by_key[key] - random.randint(1_000_000, 100_000_000)
                    env = _full_envelope(key, tenant, payload, older_ts, tx_id, record_id)
                    records.append((key, env))
                    total_lww_replay += 1
                    continue

                event_id = random.choice(key_pool)
                last_ts_by_key[event_id] = now_ns

                if roll < args.bad_parse_ratio + args.chunked_ratio:
                    # Chunked: 3-5 fragments, spread across shards via varied partition keys.
                    nfrag = random.randint(3, 5)
                    body = json.dumps(
                        {"event_id": event_id, "tenant": tenant, "payload": payload * 20, "updated_ts": now_ns}
                    )
                    parts = _split(body, nfrag)
                    chunk_id = f"c-{uuid.uuid4().hex[:12]}"
                    crc = str(_crc32c(body.encode("utf-8")))
                    if random.random() < args.bad_crc_ratio:
                        crc = "0"  # intentionally wrong
                        total_crc_bad += 1
                    main_env = _chunked_main(event_id, chunk_id, nfrag, crc, now_ns, tx_id, record_id)
                    # Main goes to one shard (via event_id key).
                    records.append((event_id, main_env))
                    # Fragments spread across shards via DISTINCT partition keys.
                    for idx, part in enumerate(parts):
                        pk = f"frag-{chunk_id}-{idx}-{random.randint(0, 1_000_000)}"
                        records.append((pk, _fragment(chunk_id, idx, part)))
                    total_chunked += 1
                else:
                    # Normal full record.
                    env = _full_envelope(event_id, tenant, payload, now_ns, tx_id, record_id)
                    records.append((event_id, env))
                # Bump now_ns so each record in the same batch has a distinct ts.
                now_ns += 1

            # Rewrite the raw-bad entries into bytes that will fail parse.
            serialized: list[dict] = []
            for pk, env in records:
                if "__raw__" in env:
                    serialized.append({"Data": env["__raw__"].encode("utf-8"), "PartitionKey": pk})
                else:
                    serialized.append({"Data": json.dumps(env).encode("utf-8"), "PartitionKey": pk})

            put_failed_hard = False
            for i in range(0, len(serialized), 250):
                chunk = serialized[i : i + 250]
                for retry_outer in range(3):
                    try:
                        resp = await kin.put_records(StreamName=args.stream_name, Records=chunk)
                        failed = int(resp.get("FailedRecordCount", 0))
                        if failed:
                            retry = [
                                chunk[j] for j, r in enumerate(resp.get("Records", [])) if "ErrorCode" in r
                            ]
                            if retry:
                                await asyncio.sleep(0.1)
                                await kin.put_records(StreamName=args.stream_name, Records=retry)
                        break
                    except Exception as exc:
                        msg = str(exc)
                        if "ExpiredToken" in msg or "RequestExpired" in msg or "InvalidClientTokenId" in msg:
                            print(f"[{int(time.time())}] creds expired — recreating kinesis client: {msg[:120]}")
                            try:
                                await kin.close()
                            except Exception:
                                pass
                            # Rebuild the client via a fresh session (reads shared creds file).
                            new_session = aioboto3.Session()
                            kin = await new_session.client("kinesis", region_name=args.region).__aenter__()
                            continue
                        print(f"[{int(time.time())}] put_records transient error: {msg[:200]}")
                        await asyncio.sleep(1.0 * (retry_outer + 1))
                else:
                    put_failed_hard = True
                    break
            if put_failed_hard:
                print(f"[{int(time.time())}] skipping batch after exhausted retries")
                continue
            total_published += len(serialized)

            if now - last_report >= 10.0:
                print(
                    f"[{int(time.time())}] elapsed={int(elapsed)}s "
                    f"cur_tps={cur_tps:.1f} published={total_published} "
                    f"chunked={total_chunked} parse_bad={total_parse_bad} "
                    f"crc_bad={total_crc_bad} lww_replay={total_lww_replay}"
                )
                last_report = now

            # Sleep so we don't overshoot the target rate on a fast loop.
            await asyncio.sleep(batch_window_s)

        print(
            f"[{int(time.time())}] DONE elapsed={int(time.monotonic() - t0)}s "
            f"published={total_published} chunked={total_chunked} "
            f"parse_bad={total_parse_bad} crc_bad={total_crc_bad} lww_replay={total_lww_replay}"
        )


if __name__ == "__main__":
    asyncio.run(main())
