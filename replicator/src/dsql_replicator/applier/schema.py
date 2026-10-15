"""Target-side table metadata: primary-key columns and column data_types.

Queried once per `(schema, table)` via `information_schema` and cached in-process.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import psycopg
import structlog

log = structlog.get_logger("applier.schema")


@dataclass(frozen=True)
class ColumnMeta:
    name: str
    data_type: str
    is_pk: bool


@dataclass(frozen=True)
class TableMeta:
    schema: str
    name: str
    pk_columns: tuple[str, ...]
    columns: tuple[ColumnMeta, ...]

    @property
    def qualified(self) -> str:
        return f'"{self.schema}"."{self.name}"'

    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def type_of(self, column: str) -> str | None:
        for c in self.columns:
            if c.name == column:
                return c.data_type
        return None


_QUERY = """
WITH pk AS (
    SELECT k.column_name
    FROM information_schema.table_constraints t
    JOIN information_schema.key_column_usage k
      ON t.constraint_name = k.constraint_name
     AND t.table_schema   = k.table_schema
    WHERE t.table_schema  = %s
      AND t.table_name    = %s
      AND t.constraint_type = 'PRIMARY KEY'
)
SELECT c.column_name,
       c.data_type,
       (pk.column_name IS NOT NULL) AS is_pk
FROM information_schema.columns c
LEFT JOIN pk ON pk.column_name = c.column_name
WHERE c.table_schema = %s
  AND c.table_name   = %s
ORDER BY c.ordinal_position
"""


class SchemaCache:
    """Lazy-loading cache of target-table metadata. Safe for concurrent callers.

    Supports connection refresh: if the owner (Applier) replaces the underlying
    connection (e.g. after a token-expiry close), it should call ``set_conn`` so
    subsequent lookups use the fresh connection. On connection errors mid-query,
    the owner is expected to retry the ``get`` call after refreshing.
    """

    def __init__(self, conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
        self._conn = conn
        self._cache: dict[tuple[str, str], TableMeta] = {}
        # Concurrent callers share one connection; serialize queries so psycopg3
        # doesn't see concurrent cursors on the same AsyncConnection.
        self._query_lock = asyncio.Lock()

    def set_conn(self, conn: psycopg.AsyncConnection[tuple[object, ...]]) -> None:
        """Swap in a freshly-opened connection after the previous one died."""
        self._conn = conn

    async def get(self, schema: str, table: str) -> TableMeta:
        key = (schema, table)
        if key in self._cache:
            return self._cache[key]
        async with self._query_lock:
            if key in self._cache:  # another caller filled it while we waited
                return self._cache[key]
            meta = await self._load(schema, table)
            self._cache[key] = meta
            return meta

    async def _load(self, schema: str, table: str) -> TableMeta:
        async with self._conn.cursor() as cur:
            await cur.execute(_QUERY, (schema, table, schema, table))
            rows = await cur.fetchall()
        if not rows:
            raise SchemaNotFoundError(f'target has no table "{schema}"."{table}"')
        columns = tuple(ColumnMeta(name=str(r[0]), data_type=str(r[1]), is_pk=bool(r[2])) for r in rows)
        pk_columns = tuple(c.name for c in columns if c.is_pk)
        if not pk_columns:
            raise SchemaNotFoundError(
                f'target table "{schema}"."{table}" has no primary key; applier requires one'
            )
        meta = TableMeta(schema=schema, name=table, pk_columns=pk_columns, columns=columns)
        log.info(
            "schema.loaded",
            schema=schema,
            table=table,
            pk=list(pk_columns),
            columns=len(columns),
        )
        return meta

    def invalidate(self, schema: str, table: str) -> None:
        self._cache.pop((schema, table), None)


class SchemaNotFoundError(Exception):
    """Raised when the target has no such table or the table has no primary key."""
