"""DSQL IAM-token-authenticated psycopg3 connection factory.

DSQL does not accept static passwords — connections authenticate with a short-lived IAM
token minted via `dsql:GenerateDbConnectAdminAuthToken`. Tokens live ~15 minutes; we cache
with a 10-minute refresh TTL and regenerate on any connection failure.
"""

from __future__ import annotations

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
    """Caches a DSQL IAM auth token for one endpoint, with monotonic TTL."""

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

    def get(self, *, force_refresh: bool = False) -> str:
        now = time.monotonic()
        if force_refresh or self._token is None or now - self._issued_mono > self._ttl:
            self._token = self._client.generate_db_connect_admin_auth_token(
                Hostname=self._endpoint.hostname, Region=self._endpoint.region
            )
            self._issued_mono = now
            log.debug("dsql.token.refresh", host=self._endpoint.hostname)
        assert self._token is not None  # for mypy
        return self._token


async def open_connection(
    endpoint: DsqlEndpoint, token_cache: DsqlTokenCache, *, autocommit: bool = False
) -> psycopg.AsyncConnection[tuple[object, ...]]:
    """Open an async psycopg connection to a DSQL endpoint using a fresh/cached IAM token."""
    return await psycopg.AsyncConnection.connect(
        host=endpoint.hostname,
        port=5432,
        dbname="postgres",
        user="admin",
        password=token_cache.get(),
        sslmode="require",
        autocommit=autocommit,
    )
