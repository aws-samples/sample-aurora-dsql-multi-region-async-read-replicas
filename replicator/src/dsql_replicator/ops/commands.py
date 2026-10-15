"""Operator click subgroup for DLQ fence management (M3).

All subcommands talk to target DSQL (fence + payload tables) and SNS (notifications).
They do NOT connect to the running consumer process — fence state is the only shared
channel, polled by the consumer's `_fence_poller` every `FENCE_POLL_INTERVAL_MS`.

Commands:
  dlq list [--status open|closed|skipped]
  dlq inspect <tx_id>
  dlq retry <tx_id>                 — re-apply + close fence atomically
  dlq skip <tx_id> --confirm --approved-by <id>
                                    — close fence without applying (REQUIRE_SKIP_SECOND_APPROVAL
                                      forbids approver == invoker in prod)
  dlq edit-and-retry <tx_id>        — $EDITOR on payload JSON, then retry
  pause                             — open a synthetic fence at ts_ns=0 (halts everything)
  unpause                           — close the synthetic fence
"""

from __future__ import annotations

import asyncio
import contextlib
import getpass
import json
import os
import subprocess
import sys
import tempfile
from typing import Any

import click
import structlog
from psycopg.types.json import Jsonb

from dsql_replicator.applier import Applier, ApplyError
from dsql_replicator.applier.connection import DsqlEndpoint
from dsql_replicator.config import Config
from dsql_replicator.dlq import DlqWriter, FenceStatus, deserialize_txn
from dsql_replicator.dlq.writer import _serialize_txn
from dsql_replicator.errors import ReplicatorError

log = structlog.get_logger("ops")

_MANUAL_PAUSE_TX_ID = "__manual_pause__"


@click.group()
def ops() -> None:
    """Operator tools: DLQ, pause/unpause."""


@ops.group()
def dlq() -> None:
    """DLQ inspection and resolution."""


@dlq.command("list")
@click.option(
    "--status",
    type=click.Choice(["open", "closed", "skipped", "all"], case_sensitive=False),
    default="open",
    help="Filter by fence status (default: open).",
)
def dlq_list(status: str) -> None:
    """List DLQ fences."""
    _run(_cmd_list(status))


@dlq.command("inspect")
@click.argument("tx_id")
def dlq_inspect(tx_id: str) -> None:
    """Print a fence row and its stored payload."""
    _run(_cmd_inspect(tx_id))


@dlq.command("retry")
@click.argument("tx_id")
def dlq_retry(tx_id: str) -> None:
    """Re-apply the stored payload and close the fence atomically."""
    _run(_cmd_retry(tx_id, edited_payload=None))


@dlq.command("skip")
@click.argument("tx_id")
@click.option("--confirm", is_flag=True, required=True, help="Required acknowledgement.")
@click.option("--approved-by", default=None, help="Second approver (required in prod).")
def dlq_skip(tx_id: str, confirm: bool, approved_by: str | None) -> None:
    """Close the fence without applying. Logs loudly; audit-safe."""
    _ = confirm
    _run(_cmd_skip(tx_id, approved_by))


@dlq.command("edit-and-retry")
@click.argument("tx_id")
def dlq_edit_and_retry(tx_id: str) -> None:
    """Open stored payload in $EDITOR, then retry with the edited version."""
    _run(_cmd_edit_and_retry(tx_id))


@ops.command("pause")
def pause_cmd() -> None:
    """Halt the applier via a synthetic fence at ts_ns=0."""
    _run(_cmd_pause())


@ops.command("unpause")
def unpause_cmd() -> None:
    """Close the synthetic manual-pause fence."""
    _run(_cmd_unpause())


# ---- async command bodies ------------------------------------------------


async def _cmd_list(status: str) -> None:
    cfg, writer = await _connect()
    try:
        status_filter: FenceStatus | None = None
        if status.lower() != "all":
            status_filter = FenceStatus(status.upper())
        rows = await writer.list_fences(status=status_filter)
        if not rows:
            click.echo("(no fences)")
            return
        click.echo(f"{'STATUS':<9}{'TS_NS':<22}{'ERROR_CLASS':<32}TX_ID")
        for r in rows:
            click.echo(f"{r.status.value:<9}{r.fence_ts_ns:<22}{r.error_class:<32}{r.tx_id}")
    finally:
        await writer.close()
        _ = cfg


async def _cmd_inspect(tx_id: str) -> None:
    cfg, writer = await _connect()
    try:
        rows = await writer.list_fences()
        fence = next((r for r in rows if r.tx_id == tx_id), None)
        if fence is None:
            click.echo(f"no fence found for tx_id={tx_id}", err=True)
            sys.exit(3)
        payload = await writer.fetch_payload(tx_id)
        click.echo(
            json.dumps(
                {
                    "fence": {
                        "tx_id": fence.tx_id,
                        "fence_ts_ns": fence.fence_ts_ns,
                        "opened_at": fence.opened_at.isoformat(),
                        "status": fence.status.value,
                        "error_class": fence.error_class,
                        "failing_sql": fence.failing_sql,
                    },
                    "payload": payload,
                },
                indent=2,
                default=str,
            )
        )
    finally:
        await writer.close()
        _ = cfg


async def _cmd_retry(tx_id: str, *, edited_payload: dict[str, Any] | None) -> None:
    cfg, writer = await _connect()
    try:
        payload = edited_payload if edited_payload is not None else await writer.fetch_payload(tx_id)
        if payload is None:
            click.echo(f"no payload stored for tx_id={tx_id}", err=True)
            sys.exit(3)
        txn = deserialize_txn(payload)
        applier = Applier(_target_endpoint(cfg))
        await applier.start()
        try:

            async def _close(cur: Any) -> None:
                updated = await writer.close_fence_in_cursor(cur, tx_id)
                if updated == 0:
                    raise ApplyError(f"no OPEN fence for tx_id={tx_id}; retry aborted")

            result = await applier.apply(txn, pre_commit_hook=_close)
            await writer.publish_closed(tx_id)
            # Persist the edited payload for forensic traceability.
            if edited_payload is not None:
                await _overwrite_payload(writer, tx_id, edited_payload)
            click.echo(
                f"retry.ok tx_id={tx_id} rows_applied={result.rows_applied} "
                f"dedup={result.rows_deduped} attempt={result.attempt}"
            )
        finally:
            await applier.close()
    finally:
        await writer.close()


async def _cmd_skip(tx_id: str, approver: str | None) -> None:
    cfg, writer = await _connect()
    try:
        invoker = getpass.getuser()
        effective_approver = approver or invoker
        if cfg.require_skip_second_approval:
            if not approver:
                click.echo(
                    "REQUIRE_SKIP_SECOND_APPROVAL=true — pass --approved-by <second-operator>",
                    err=True,
                )
                sys.exit(4)
            if approver == invoker:
                click.echo(
                    f"--approved-by must differ from invoker ({invoker})",
                    err=True,
                )
                sys.exit(4)
        updated = await writer.skip_fence(tx_id, approver=effective_approver)
        if not updated:
            click.echo(f"no OPEN fence for tx_id={tx_id}", err=True)
            sys.exit(3)
        click.echo(f"skip.ok tx_id={tx_id} approver={effective_approver}")
    finally:
        await writer.close()


async def _cmd_edit_and_retry(tx_id: str) -> None:
    cfg, writer = await _connect()
    try:
        payload = await writer.fetch_payload(tx_id)
        if payload is None:
            click.echo(f"no payload stored for tx_id={tx_id}", err=True)
            sys.exit(3)
    finally:
        await writer.close()
    _ = cfg
    edited = _edit_json(payload)
    await _cmd_retry(tx_id, edited_payload=edited)


async def _cmd_pause() -> None:
    cfg, writer = await _connect()
    try:
        conn = writer._require_conn()
        async with conn.transaction(), conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO __replicator_dlq_fences "
                "(tx_id, fence_ts_ns, status, error_class, failing_sql) "
                "VALUES (%s, 0, %s, %s, %s) "
                "ON CONFLICT (tx_id) DO UPDATE SET "
                "  status      = EXCLUDED.status, "
                "  error_class = EXCLUDED.error_class, "
                "  failing_sql = EXCLUDED.failing_sql, "
                "  opened_at   = now(), "
                "  closed_at   = NULL",
                (
                    _MANUAL_PAUSE_TX_ID,
                    FenceStatus.OPEN.value,
                    "MANUAL_PAUSE",
                    "manual pause via `dsql-replicator ops pause`",
                ),
            )
        await writer._publish_sns(
            event_type="opened", tx_id=_MANUAL_PAUSE_TX_ID, fence_ts_ns=0, error_class="MANUAL_PAUSE"
        )
        click.echo("pause.ok (manual fence at ts_ns=0)")
    finally:
        await writer.close()
    _ = cfg


async def _cmd_unpause() -> None:
    cfg, writer = await _connect()
    try:
        conn = writer._require_conn()
        async with conn.transaction(), conn.cursor() as cur:
            await cur.execute(
                "UPDATE __replicator_dlq_fences "
                "SET status = %s, closed_at = now() "
                "WHERE tx_id = %s AND status = %s",
                (FenceStatus.CLOSED.value, _MANUAL_PAUSE_TX_ID, FenceStatus.OPEN.value),
            )
            updated = int(cur.rowcount)
        if updated:
            await writer.publish_closed(_MANUAL_PAUSE_TX_ID)
            click.echo("unpause.ok")
        else:
            click.echo("unpause.noop (no manual pause fence was OPEN)")
    finally:
        await writer.close()
    _ = cfg


# ---- helpers --------------------------------------------------------------


async def _connect() -> tuple[Config, DlqWriter]:
    cfg = _load_cfg()
    writer = DlqWriter(cfg, _target_endpoint(cfg))
    await writer.start()
    return cfg, writer


async def _overwrite_payload(writer: DlqWriter, tx_id: str, edited: dict[str, Any]) -> None:
    conn = writer._require_conn()
    async with conn.transaction(), conn.cursor() as cur:
        await cur.execute(
            "UPDATE __replicator_dlq_payload SET payload_json = %s WHERE tx_id = %s",
            (Jsonb(edited), tx_id),
        )
    _ = _serialize_txn  # keep import live for tests that patch it


def _edit_json(payload: dict[str, Any]) -> dict[str, Any]:
    editor = os.environ.get("EDITOR", "vi")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(payload, f, indent=2, default=str)
        path = f.name
    try:
        subprocess.run([editor, path], check=True)
        with open(path) as f:
            return dict(json.load(f))
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)


def _load_cfg() -> Config:
    try:
        return Config.from_env()
    except ReplicatorError as exc:
        click.echo(f"error: {exc}", err=True)
        sys.exit(2)


def _target_endpoint(cfg: Config) -> DsqlEndpoint:
    host = f"{cfg.target_cluster_id}.dsql.{cfg.target_region}.on.aws"
    return DsqlEndpoint(
        hostname=host,
        region=cfg.target_region,
        endpoint_url=cfg.target_endpoint_url,
        cluster_identifier=cfg.target_cluster_id,
    )


def _run(coro: Any) -> None:
    try:
        asyncio.run(coro)
    except ApplyError as exc:
        click.echo(f"apply error: {exc}", err=True)
        sys.exit(1)
    except ReplicatorError as exc:
        click.echo(f"error: {exc}", err=True)
        sys.exit(1)
