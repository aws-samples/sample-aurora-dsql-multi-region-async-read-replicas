"""DSQL IAM-token-authenticated psycopg3 connection factory.

DSQL does not accept static passwords — connections authenticate with a short-lived IAM
token minted via `dsql:GenerateDbConnectAdminAuthToken`. Tokens live ~15 minutes; we cache
with a 10-minute refresh TTL and regenerate on any connection failure.

Concurrency contract (post-SSL-EOF-storm fix, 2026-10-26):

- `DsqlTokenCache.get_async()` is the only safe entry point from async code. It is
  guarded by an asyncio.Lock so N concurrent callers trigger at most ONE token-mint
  call to the DSQL control plane. The rest wait and read the cached result.
- The sync `generate_db_connect_admin_auth_token` boto3 call is offloaded to a
  thread via `asyncio.to_thread` so SigV4 signing never blocks the event loop.
- The old sync `.get()` is kept for the handful of non-async call sites (bootstrap,
  tests) but no longer safe to call from a hot async path.

Why this matters: without the lock, 128 pool connections opening at startup made
128 simultaneous `generate_db_connect_admin_auth_token` calls. Control-plane
throttling + sync boto3 blocking the event loop produced a correlated burst of
`SSL error: unexpected eof while reading` apply failures and opened fences.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import boto3
import psycopg
import structlog

log = structlog.get_logger("applier.connection")


@dataclass(frozen=True)
class DsqlEndpoint:
    """Describes a single DSQL cluster endpoint."""

    hostname: str
    region: str
    endpoint_url: str | None = None
    cluster_identifier: str | None = None  # bookkeeping only


class DsqlTokenCache:
    """Caches a DSQL IAM auth token for one endpoint, with monotonic TTL.

    Async-safe: concurrent `get_async()` callers serialize on an internal
    asyncio.Lock so only one SigV4 signing call runs at a time. The signing
    itself runs in a thread via `asyncio.to_thread` so the event loop stays
    responsive during the ~10 ms boto3 call.
    """

    def __init__(self, endpoint: DsqlEndpoint, ttl_seconds: float = 600.0) -> None:
        self._endpoint = endpoint
        self._ttl = ttl_seconds
        kwargs: dict[str, str] = {"region_name": endpoint.region}
        if endpoint.endpoint_url:
            kwargs["endpoint_url"] = endpoint.endpoint_url
        # DSQL is not yet in botocore type stubs; fall back to untyped client.
        self._client = boto3.client("dsql", **kwargs)  # type: ignore[call-overload]
        self._token: str | None = None
        self._issued_mono: float = 0.0
        self._lock = asyncio.Lock()

    def _needs_refresh(self, now: float, force: bool) -> bool:
        return force or self._token is None or (now - self._issued_mono) > self._ttl

    def _mint_sync(self) -> str:
        """Blocking SigV4-signed token mint. Called from a worker thread."""
        return self._client.generate_db_connect_admin_auth_token(  # type: ignore[no-any-return]
            Hostname=self._endpoint.hostname, Region=self._endpoint.region
        )

    async def get_async(self, *, force_refresh: bool = False) -> str:
        """Async-safe token read. The lock serializes concurrent refreshes so
        N racing callers only trigger ONE control-plane call."""
        now = time.monotonic()
        if not self._needs_refresh(now, force_refresh):
            assert self._token is not None  # for mypy
            return self._token
        async with self._lock:
            # Re-check under the lock — another coroutine may have refreshed while
            # we were waiting to acquire it.
            now = time.monotonic()
            if not self._needs_refresh(now, force_refresh):
                assert self._token is not None
                return self._token
            self._token = await asyncio.to_thread(self._mint_sync)
            self._issued_mono = time.monotonic()
            log.info(
                "dsql.token.refresh",
                host=self._endpoint.hostname,
                forced=force_refresh,
            )
            return self._token

    def get(self, *, force_refresh: bool = False) -> str:
        """Sync entry point. Safe only from non-async contexts (bootstrap, tests).

        If an event loop is running on this thread, we'd re-enter it with
        `asyncio.run`, which would deadlock. Use `get_async` from async code.
        """
        now = time.monotonic()
        if not self._needs_refresh(now, force_refresh):
            assert self._token is not None
            return self._token
        self._token = self._mint_sync()
        self._issued_mono = time.monotonic()
        log.info(
            "dsql.token.refresh",
            host=self._endpoint.hostname,
            forced=force_refresh,
            sync=True,
        )
        return self._token


async def open_connection(
    endpoint: DsqlEndpoint, token_cache: DsqlTokenCache, *, autocommit: bool = False
) -> psycopg.AsyncConnection[tuple[object, ...]]:
    """Open an async psycopg connection to a DSQL endpoint using a fresh/cached IAM token."""
    password = await token_cache.get_async()
    return await psycopg.AsyncConnection.connect(
        host=endpoint.hostname,
        port=5432,
        dbname="postgres",
        user="admin",
        password=password,
        sslmode="require",
        autocommit=autocommit,
    )
