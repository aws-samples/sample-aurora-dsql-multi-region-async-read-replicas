"""Top-level click entry point. Dispatches to migrate / consume / ops."""

from __future__ import annotations

import asyncio
import json
import sys

import click

from . import consumer, health, logging_setup, migrate
from .config import Config
from .errors import ReplicatorError
from .ops.commands import ops as ops_group


@click.group()
@click.version_option(package_name="dsql-replicator")
def main() -> None:
    """Aurora DSQL multi-region async replicator."""
    logging_setup.init()


@main.command("migrate")
def migrate_cmd() -> None:
    """Idempotently provision DDB tables + SQS DLQ + CloudWatch dashboard."""
    cfg = _load_config()
    report = migrate.run(cfg)
    click.echo(json.dumps(report.to_dict(), indent=2, default=str))


@main.command("consume")
def consume_cmd() -> None:
    """Run the consumer / applier pipeline.

    If `REPLICATOR_PROCESSES > 1`, this command becomes a thin supervisor over N
    child consumer processes (one asyncio loop each) to escape the Python GIL
    ceiling. Lease arbitration across children is handled by the existing
    __replicator_shard_leases table — no coordination code required.
    """
    cfg = _load_config()
    try:
        if cfg.replicator_processes > 1:
            # Imported lazily so single-process runs don't pay the cost.
            from .consumer.supervisor import run_supervised

            run_supervised(cfg.replicator_processes)  # calls sys.exit internally
            return
        asyncio.run(consumer.run(cfg))
    except ReplicatorError as exc:
        click.echo(f"error: {exc}", err=True)
        sys.exit(1)


@main.command("healthz")
def healthz_cmd() -> None:
    """Probe target DSQL + DDB + SNS; exit 0 if all probes pass. Used by Fargate HEALTHCHECK."""
    cfg = _load_config()
    results = asyncio.run(health.run_healthz(cfg))
    all_ok = all(r.ok for r in results)
    for r in results:
        marker = "ok" if r.ok else "FAIL"
        click.echo(f"{marker} {r.name}: {r.detail}")
    sys.exit(0 if all_ok else 1)


main.add_command(ops_group)


def _load_config() -> Config:
    try:
        return Config.from_env()
    except ReplicatorError as exc:
        click.echo(f"error: {exc}", err=True)
        sys.exit(2)


if __name__ == "__main__":
    main()
