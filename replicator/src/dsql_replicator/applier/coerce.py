"""Coerce JSON values from CDC payloads into PostgreSQL-ready Python values.

DSQL CDC serializes some PG types in forms that psycopg will not auto-convert back:

- `timestamp` / `timestamptz` → integer microseconds since the Unix epoch.
- `date` → integer days since 1970-01-01.
- `time` / `timetz` → integer microseconds since midnight.
- `bytea` → Base64 string.
- `numeric` / `decimal` → JSON string.
- `json` / `jsonb` → JSON string (raw JSON text as a string).
- `interval` → integer approximate microseconds (may be `null` on overflow — pass through).
- `uuid`, `text`, `varchar`, numeric primitives → pass through.

This module maps PG `data_type` names (as returned by `information_schema.columns.data_type`)
to a coercer function. Unknown types pass through unchanged so psycopg's default
converters can handle them.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from psycopg.types.json import Json, Jsonb

Coercer = Callable[[Any], Any]


def _passthrough(v: Any) -> Any:
    return v


def _timestamp(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, int):
        # Microseconds since Unix epoch.
        return datetime.fromtimestamp(v / 1_000_000, tz=UTC)
    return v


def _date(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, int):
        return date(1970, 1, 1) + timedelta(days=v)
    return v


def _time(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, int):
        # Microseconds since midnight.
        hours, rem = divmod(v, 3_600_000_000)
        minutes, rem = divmod(rem, 60_000_000)
        seconds, micros = divmod(rem, 1_000_000)
        return time(hour=int(hours), minute=int(minutes), second=int(seconds), microsecond=int(micros))
    return v


def _bytea(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        return base64.b64decode(v)
    return v


def _numeric(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        return Decimal(v)
    return v


def _json(v: Any) -> Any:
    """DSQL emits the raw JSON text as a JSON string. Wrap in psycopg's Json so the
    adapter sends it to a PG `json` column (and not as a bare text literal)."""
    if v is None:
        return None
    parsed = json.loads(v) if isinstance(v, str) else v
    return Json(parsed)


def _jsonb(v: Any) -> Any:
    """Same as `_json` but targets `jsonb`."""
    if v is None:
        return None
    parsed = json.loads(v) if isinstance(v, str) else v
    return Jsonb(parsed)


def _interval(v: Any) -> Any:
    """Pass as microseconds-as-timedelta when integer. May be null on overflow."""
    if v is None:
        return None
    if isinstance(v, int):
        return timedelta(microseconds=v)
    return v


_BY_DATA_TYPE: dict[str, Coercer] = {
    "timestamp with time zone": _timestamp,
    "timestamp without time zone": _timestamp,
    "date": _date,
    "time without time zone": _time,
    "time with time zone": _time,
    "bytea": _bytea,
    "numeric": _numeric,
    "json": _json,
    "jsonb": _jsonb,
    "interval": _interval,
}


def coerce_value(data_type: str, value: Any) -> Any:
    """Coerce one JSON-decoded value for one PG column."""
    fn = _BY_DATA_TYPE.get(data_type, _passthrough)
    return fn(value)
