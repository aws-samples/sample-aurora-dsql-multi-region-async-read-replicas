"""Bootstrap the target-side demo schema the synthetic publisher writes to.

A single table `public.loadtest_events` matching the envelopes the publisher
emits. Running separately from `dsql-replicator migrate` because migrate only
upserts the dashboard, not application tables.
"""

from __future__ import annotations

import asyncio
import os

from dsql_replicator.applier.connection import DsqlEndpoint, DsqlTokenCache, open_connection


DDL = """
CREATE TABLE IF NOT EXISTS loadtest_events (
    event_id    text PRIMARY KEY,
    tenant      text NOT NULL,
    payload     text NOT NULL,
    updated_ts  bigint NOT NULL
)
"""


async def _run() -> None:
    cluster_id = os.environ["TARGET_CLUSTER_ID"]
    region = os.environ["TARGET_REGION"]
    endpoint = DsqlEndpoint(
        hostname=f"{cluster_id}.dsql.{region}.on.aws",
        region=region,
        cluster_identifier=cluster_id,
    )
    conn = await open_connection(endpoint, DsqlTokenCache(endpoint), autocommit=False)
    try:
        async with conn.cursor() as cur:
            await cur.execute(DDL)
        await conn.commit()
        print("loadtest_events table ready")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(_run())
