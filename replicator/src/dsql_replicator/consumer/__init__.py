"""Consumer pipeline: Kinesis → envelope → reassembly → ordering → applier.

M4: `SOURCE_STREAMS` may contain multiple streams. One `ConsumerPipeline` is created
per stream; all pipelines share nothing by default — each has its own Applier (which
means two commit lanes). Spec I8 (global `ts_ns` ordering) is enforced only WITHIN a
stream, not across streams. If you need cross-stream ordering, run one process per
pair of ordered streams or revisit the Applier-sharing design.
"""

from __future__ import annotations

import asyncio
import contextlib

from dsql_replicator.applier.connection import DsqlEndpoint
from dsql_replicator.config import Config

from .pipeline import ConsumerPipeline


async def run(cfg: Config) -> None:
    """Entry point used by `dsql-replicator consume`.

    Runs one `ConsumerPipeline` per stream in `SOURCE_STREAMS`. On any pipeline raising,
    all pipelines are stopped and the first exception is re-raised.
    """
    target = DsqlEndpoint(
        hostname=_target_hostname(cfg),
        region=cfg.target_region,
        endpoint_url=cfg.target_endpoint_url,
        cluster_identifier=cfg.target_cluster_id,
    )
    pipelines = [ConsumerPipeline(cfg, stream_name=s, target_endpoint=target) for s in cfg.source_streams]
    tasks = [asyncio.create_task(p.run(), name=f"pipeline:{p.stream_name}") for p in pipelines]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        # If one pipeline exited (clean or error), stop the rest.
        for p in pipelines:
            p.stop()
        for t in pending:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        # Re-raise the first exception if any.
        for t in done:
            exc = t.exception()
            if exc is not None:
                raise exc
    finally:
        for t in tasks:
            t.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.gather(*tasks, return_exceptions=True)


def _target_hostname(cfg: Config) -> str:
    return f"{cfg.target_cluster_id}.dsql.{cfg.target_region}.on.aws"


__all__ = ["ConsumerPipeline", "run"]
