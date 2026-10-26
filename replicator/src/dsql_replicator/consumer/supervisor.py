"""Multi-process supervisor (pattern C).

`REPLICATOR_PROCESSES > 1` → spawn N child processes, each running the single-
process `consumer.run(cfg)` path in its own asyncio event loop. The GIL stops
being the per-task throughput ceiling.

Shard ownership is NOT partitioned by the supervisor. All children race for
every shard via the existing `__replicator_shard_leases` table; the lease
primitive guarantees exactly one owner per shard at any time. Self-balances
automatically if a child dies — surviving children steal its leases on expiry.

Isolation:
- `spawn` start method only (never `fork`): we don't want a copied asyncio
  loop, boto3 session, or psycopg connection pool in the child.
- Each child gets `PROCESS_ID=<i>` and `TASK_ID=<base>-p<i>` so logs, lease
  rows, and the per-process liveness file are distinguishable.

Lifecycle:
- Parent is a thin supervisor: starts children, watches, propagates signals.
- SIGTERM / SIGINT → terminate all children, then join with a short grace
  period before SIGKILL.
- If any child exits non-zero while the supervisor is not stopping, the
  supervisor terminates the rest and exits with the child's exit code so
  Fargate's deployment circuit breaker can roll back.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import signal
import sys
import time
from multiprocessing.context import SpawnProcess
from types import FrameType

log = logging.getLogger("supervisor")

_JOIN_GRACE_S = 10.0
_KILL_GRACE_S = 5.0
_POLL_INTERVAL_S = 0.5


def _child_entry(process_id: int, base_task_id: str) -> None:
    """Child-process entry point. Runs the single-process consumer under its own
    PROCESS_ID; the per-process liveness file lives at `.../liveness-{PROCESS_ID}`."""
    # Imports deferred so they happen in the fresh spawned interpreter.
    import asyncio

    from dsql_replicator import consumer, logging_setup
    from dsql_replicator.config import Config
    from dsql_replicator.errors import ReplicatorError

    os.environ["PROCESS_ID"] = str(process_id)
    if base_task_id:
        os.environ["TASK_ID"] = f"{base_task_id}-p{process_id}"

    logging_setup.init()
    try:
        cfg = Config.from_env()
        asyncio.run(consumer.run(cfg))
    except ReplicatorError as exc:
        print(f"[p{process_id}] replicator error: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(0)


def run_supervised(n_processes: int) -> None:
    """Spawn N children and supervise. Blocks until all children exit; calls
    `sys.exit(code)` with an aggregate code so ECS sees pass/fail correctly."""
    if n_processes < 1:
        raise ValueError(f"n_processes must be >= 1, got {n_processes}")

    ctx = mp.get_context("spawn")
    base_task_id = os.environ.get("TASK_ID", "")

    children: list[SpawnProcess] = []
    for i in range(n_processes):
        p = ctx.Process(
            target=_child_entry,
            args=(i, base_task_id),
            name=f"replicator-p{i}",
            daemon=False,
        )
        p.start()
        children.append(p)
        print(f"[supervisor] spawned child p{i} pid={p.pid}", flush=True)

    stopping = {"flag": False}

    def _stop(_sig: int, _frame: FrameType | None) -> None:
        if stopping["flag"]:
            return
        stopping["flag"] = True
        print(f"[supervisor] received signal {_sig}, terminating children", flush=True)
        for c in children:
            if c.is_alive():
                c.terminate()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _stop)

    exit_code = 0
    while True:
        all_done = True
        for c in children:
            if c.is_alive():
                all_done = False
                continue
            # Child exited.
            if c.exitcode and c.exitcode != 0 and not stopping["flag"]:
                print(
                    f"[supervisor] child {c.name} exited unexpectedly code={c.exitcode}; "
                    "terminating siblings",
                    flush=True,
                )
                exit_code = c.exitcode
                stopping["flag"] = True
                for other in children:
                    if other is not c and other.is_alive():
                        other.terminate()
        if all_done:
            break
        time.sleep(_POLL_INTERVAL_S)

    # Final join + SIGKILL grace.
    for c in children:
        c.join(timeout=_JOIN_GRACE_S)
        if c.is_alive():
            print(f"[supervisor] force-killing {c.name} pid={c.pid}", flush=True)
            c.kill()
            c.join(timeout=_KILL_GRACE_S)
        if exit_code == 0 and c.exitcode and c.exitcode != 0:
            exit_code = c.exitcode

    print(f"[supervisor] all children exited; code={exit_code}", flush=True)
    sys.exit(exit_code)
