#!/usr/bin/env python3
"""Steady-rate DSQL load generator for pre-M1 benchmarking.

Spawns N async workers that commit transactions against the source DSQL cluster
at a fixed aggregate transactions-per-minute target. Each transaction performs
2-5 random DMLs (insert/update/delete) on the `{table}` table.

Reports per-minute throughput and p50/p95/p99 commit latency; dumps a final
JSON report with the full timeline.

Usage:
    env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \\
        ./generate_load.py \\
            --cluster-host <host> --region us-east-1 \\
            --endpoint-url https://dsql.us-east-1.api.aws \\
            --tpm 500 --duration-sec 900 --workers 4 \\
            --output /tmp/load-report.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import signal
import statistics
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque

import boto3
import psycopg
from psycopg.rows import tuple_row


TOKEN_REFRESH_SEC = 10 * 60  # DSQL IAM tokens live ~15 min; refresh at 10


@dataclass
class WorkerStats:
    worker_id: int
    committed: int = 0
    failed: int = 0
    latencies_ms: list[float] = field(default_factory=list)


@dataclass
class MinuteBucket:
    minute: int
    committed: int = 0
    failed: int = 0
    dmls: int = 0
    latencies_ms: list[float] = field(default_factory=list)


class TokenCache:
    def __init__(self, host: str, region: str, endpoint_url: str) -> None:
        self.host = host
        self.region = region
        self.endpoint_url = endpoint_url
        self._token: str | None = None
        self._issued_at = 0.0

    def get(self) -> str:
        if self._token is None or time.monotonic() - self._issued_at > TOKEN_REFRESH_SEC:
            # Build a FRESH boto3 Session each refresh so a long-running load gen
            # survives `ada credentials update` rotations in ~/.aws/credentials.
            # The default boto3 Session is a module-level singleton that caches
            # creds at first use — a `boto3.client(...)` alone picks the stale
            # singleton. Only a new `Session()` re-reads the credentials file.
            session = boto3.session.Session()
            client = session.client("dsql", region_name=self.region, endpoint_url=self.endpoint_url)
            self._token = client.generate_db_connect_admin_auth_token(
                Hostname=self.host, Region=self.region
            )
            self._issued_at = time.monotonic()
        return self._token


async def open_connection(host: str, token_cache: TokenCache) -> psycopg.AsyncConnection:
    pw = token_cache.get()
    return await psycopg.AsyncConnection.connect(
        host=host,
        port=5432,
        dbname="postgres",
        user="admin",
        password=pw,
        sslmode="require",
        row_factory=tuple_row,
        autocommit=True,  # we manage our own BEGIN/COMMIT per transaction
    )


def gen_payload() -> str:
    n = random.randint(16, 128)
    return "x" * n


def gen_dmls(recent_ids: Deque[uuid.UUID], table: str) -> list[tuple[str, tuple]]:
    """Build 2-5 DMLs for a single transaction. Returns (sql, params) tuples."""
    n = random.randint(2, 5)
    stmts: list[tuple[str, tuple]] = []
    for _ in range(n):
        kind = random.choices(["insert", "update", "delete"], weights=[0.6, 0.3, 0.1])[0]
        if kind == "insert" or not recent_ids:
            new_id = uuid.uuid4()
            recent_ids.append(new_id)
            stmts.append(
                (
                    f"INSERT INTO {table} (id, bucket, payload) VALUES (%s, %s, %s)",
                    (new_id, random.randint(1, 100), gen_payload()),
                )
            )
        elif kind == "update":
            target = random.choice(list(recent_ids))
            stmts.append(
                (
                    f"UPDATE {table} SET payload = %s, updated_at = now() WHERE id = %s",
                    (gen_payload(), target),
                )
            )
        else:  # delete
            target = recent_ids.popleft() if random.random() < 0.5 and recent_ids else random.choice(list(recent_ids))
            try:
                recent_ids.remove(target)
            except ValueError:
                pass
            stmts.append((f"DELETE FROM {table} WHERE id = %s", (target,)))
    return stmts


async def worker(
    worker_id: int,
    host: str,
    token_cache: TokenCache,
    target_rate_per_sec: float,
    stop_at: float,
    stats: WorkerStats,
    minutes: dict[int, MinuteBucket],
    minutes_lock: asyncio.Lock,
    started_at: float,
    table: str,
    start_delay_sec: float = 0.0,
) -> None:
    if start_delay_sec > 0:
        await asyncio.sleep(start_delay_sec)
    recent_ids: Deque[uuid.UUID] = deque(maxlen=10_000)
    conn: psycopg.AsyncConnection | None = None
    interval = 1.0 / target_rate_per_sec
    next_slot = time.monotonic()

    while time.monotonic() < stop_at:
        now = time.monotonic()
        sleep = next_slot - now
        if sleep > 0:
            await asyncio.sleep(sleep)
        next_slot += interval

        if conn is None or conn.closed:
            try:
                conn = await open_connection(host, token_cache)
            except Exception as e:  # noqa: BLE001
                stats.failed += 1
                print(f"[w{worker_id}] connect error: {e}", file=sys.stderr)
                await asyncio.sleep(1.0)
                continue

        stmts = gen_dmls(recent_ids, table)
        t0 = time.monotonic()
        try:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    for sql, params in stmts:
                        await cur.execute(sql, params)
            lat_ms = (time.monotonic() - t0) * 1000.0
            stats.committed += 1
            stats.latencies_ms.append(lat_ms)
            minute = int((time.monotonic() - started_at) // 60)
            async with minutes_lock:
                b = minutes.setdefault(minute, MinuteBucket(minute=minute))
                b.committed += 1
                b.dmls += len(stmts)
                b.latencies_ms.append(lat_ms)
        except Exception as e:  # noqa: BLE001
            stats.failed += 1
            msg = str(e)[:200]
            print(f"[w{worker_id}] txn error: {msg}", file=sys.stderr)
            minute = int((time.monotonic() - started_at) // 60)
            async with minutes_lock:
                b = minutes.setdefault(minute, MinuteBucket(minute=minute))
                b.failed += 1
            try:
                await conn.close()
            except Exception:  # noqa: BLE001
                pass
            conn = None

    if conn is not None and not conn.closed:
        try:
            await conn.close()
        except Exception:  # noqa: BLE001
            pass


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    return float(statistics.quantiles(values, n=100)[int(q) - 1]) if len(values) >= 100 else sorted(values)[min(len(values) - 1, int(len(values) * q / 100))]


async def reporter(
    stats_list: list[WorkerStats],
    minutes: dict[int, MinuteBucket],
    minutes_lock: asyncio.Lock,
    started_at: float,
    stop_at: float,
) -> None:
    reported = -1
    while time.monotonic() < stop_at:
        await asyncio.sleep(5)
        current_minute = int((time.monotonic() - started_at) // 60)
        async with minutes_lock:
            for m in sorted(minutes.keys()):
                if m <= reported or m >= current_minute:
                    continue
                b = minutes[m]
                p50 = pct(b.latencies_ms, 50)
                p95 = pct(b.latencies_ms, 95)
                p99 = pct(b.latencies_ms, 99)
                print(
                    f"[minute {m:2d}] txn/min={b.committed:5d}  failed={b.failed:3d}  dmls={b.dmls:5d}"
                    f"  p50={p50:6.1f}ms  p95={p95:6.1f}ms  p99={p99:6.1f}ms",
                    flush=True,
                )
                reported = m


async def main_async(args: argparse.Namespace) -> None:
    token_cache = TokenCache(args.cluster_host, args.region, args.endpoint_url)
    stats_list = [WorkerStats(worker_id=i) for i in range(args.workers)]
    minutes: dict[int, MinuteBucket] = {}
    minutes_lock = asyncio.Lock()

    started_at = time.monotonic()
    stop_at = started_at + args.duration_sec
    rate_per_worker = (args.tpm / 60.0) / args.workers

    print(
        f"[launch] workers={args.workers} tpm={args.tpm} duration_sec={args.duration_sec}"
        f"  rate_per_worker={rate_per_worker:.3f} txn/s",
        flush=True,
    )

    ramp_sec = max(0.0, float(getattr(args, "ramp_sec", 0.0) or 0.0))
    tasks = [
        asyncio.create_task(
            worker(
                i, args.cluster_host, token_cache, rate_per_worker, stop_at, stats_list[i],
                minutes, minutes_lock, started_at, args.table,
                start_delay_sec=(ramp_sec * i / max(1, args.workers - 1)) if ramp_sec > 0 else 0.0,
            )
        )
        for i in range(args.workers)
    ]
    if ramp_sec > 0:
        print(f"[launch] ramp_sec={ramp_sec} — worker starts spread evenly over this window", flush=True)
    reporter_task = asyncio.create_task(
        reporter(stats_list, minutes, minutes_lock, started_at, stop_at)
    )

    stop_event = asyncio.Event()

    def _stop(*_: object) -> None:
        print("\n[signal] stopping early", flush=True)
        stop_event.set()
        for t in tasks:
            t.cancel()
        reporter_task.cancel()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        reporter_task.cancel()

    total_committed = sum(s.committed for s in stats_list)
    total_failed = sum(s.failed for s in stats_list)
    all_lat = [l for s in stats_list for l in s.latencies_ms]
    elapsed = time.monotonic() - started_at

    report = {
        "started_at_wall": time.time() - elapsed,
        "ended_at_wall": time.time(),
        "duration_sec": elapsed,
        "target_tpm": args.tpm,
        "workers": args.workers,
        "total_committed": total_committed,
        "total_failed": total_failed,
        "effective_tpm": (total_committed / elapsed) * 60.0 if elapsed > 0 else 0.0,
        "latency_ms": {
            "count": len(all_lat),
            "mean": statistics.fmean(all_lat) if all_lat else 0.0,
            "p50": pct(all_lat, 50),
            "p95": pct(all_lat, 95),
            "p99": pct(all_lat, 99),
            "max": max(all_lat) if all_lat else 0.0,
        },
        "minutes": [
            {
                "minute": m,
                "committed": b.committed,
                "failed": b.failed,
                "dmls": b.dmls,
                "p50_ms": pct(b.latencies_ms, 50),
                "p95_ms": pct(b.latencies_ms, 95),
                "p99_ms": pct(b.latencies_ms, 99),
            }
            for m, b in sorted(minutes.items())
        ],
    }

    print("\n=== FINAL ===", flush=True)
    print(json.dumps({k: v for k, v in report.items() if k != "minutes"}, indent=2, default=str))
    if args.output:
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"[output] wrote {args.output}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--cluster-host", required=True)
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--endpoint-url", default="https://dsql.us-east-1.api.aws")
    p.add_argument("--tpm", type=int, default=500, help="target transactions per minute")
    p.add_argument("--duration-sec", type=int, default=900)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--ramp-sec", type=float, default=0.0, help="stagger worker starts linearly over this window")
    p.add_argument("--table", default="load_test", help="target table name for DML (must exist on source + target)")
    p.add_argument("--output", default=None)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass
