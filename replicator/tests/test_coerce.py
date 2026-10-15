"""Value coercion tests: JSON CDC payloads → PG-ready Python values."""

from __future__ import annotations

import base64
import json
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

from dsql_replicator.applier.coerce import coerce_value


def test_timestamp_micros_to_datetime() -> None:
    micros = 1_710_510_600_123_456
    out = coerce_value("timestamp with time zone", micros)
    assert isinstance(out, datetime)
    assert out.tzinfo is UTC
    # Round-trip: the µs count must equal what we'd regenerate from the parsed datetime.
    assert int(out.timestamp() * 1_000_000) == micros


def test_timestamp_passthrough_when_already_datetime() -> None:
    dt = datetime(2026, 1, 1, tzinfo=UTC)
    assert coerce_value("timestamp with time zone", dt) is dt


def test_date_days_since_epoch() -> None:
    # 19797 days since 1970-01-01 → some day in 2024.
    assert coerce_value("date", 19797) == date(1970, 1, 1) + timedelta(days=19797)


def test_time_micros_since_midnight() -> None:
    micros = 52_200_123_456  # 14:30:00.123456
    out = coerce_value("time without time zone", micros)
    assert out == time(14, 30, 0, 123456)


def test_bytea_base64_decodes() -> None:
    original = b"Hello DSQL"
    encoded = base64.b64encode(original).decode()
    assert coerce_value("bytea", encoded) == original


def test_numeric_string_to_decimal() -> None:
    assert coerce_value("numeric", "123.45") == Decimal("123.45")


def test_json_string_decoded() -> None:
    from psycopg.types.json import Json, Jsonb

    payload = {"k": "v", "n": 1}
    out_json = coerce_value("json", json.dumps(payload))
    assert isinstance(out_json, Json)
    assert out_json.obj == payload
    out_jsonb = coerce_value("jsonb", json.dumps(payload))
    assert isinstance(out_jsonb, Jsonb)
    assert out_jsonb.obj == payload


def test_interval_micros_to_timedelta() -> None:
    assert coerce_value("interval", 1_500_000) == timedelta(microseconds=1_500_000)


def test_none_pass_through_for_all_types() -> None:
    for t in [
        "timestamp with time zone",
        "date",
        "time without time zone",
        "bytea",
        "numeric",
        "json",
        "interval",
    ]:
        assert coerce_value(t, None) is None


def test_unknown_type_passes_through() -> None:
    assert (
        coerce_value("uuid", "550e8400-e29b-41d4-a716-446655440000") == "550e8400-e29b-41d4-a716-446655440000"
    )
    assert coerce_value("text", "hello") == "hello"
    assert coerce_value("integer", 42) == 42
    assert coerce_value("boolean", True) is True
