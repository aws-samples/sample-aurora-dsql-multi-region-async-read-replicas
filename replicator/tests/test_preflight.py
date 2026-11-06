from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dsql_replicator import preflight
from dsql_replicator.errors import ConfigError


def _stream(stream_id: str, status: str) -> dict:
    return {
        "clusterIdentifier": "cluster-abc",
        "streamIdentifier": stream_id,
        "arn": f"arn:aws:dsql:us-east-1:123456789012:cluster/cluster-abc/stream/{stream_id}",
        "status": status,
    }


def _get_stream_resp(stream_id: str, stream_name: str) -> dict:
    return {
        "clusterIdentifier": "cluster-abc",
        "streamIdentifier": stream_id,
        "status": "ACTIVE",
        "targetDefinition": {
            "kinesis": {
                "streamArn": f"arn:aws:kinesis:us-east-1:123456789012:stream/{stream_name}",
                "roleArn": "arn:aws:iam::123456789012:role/dsql-cdc",
            }
        },
    }


def test_no_streams_raises() -> None:
    client = MagicMock()
    client.list_streams.return_value = {"streams": []}
    with pytest.raises(ConfigError, match="CDC is not enabled"):
        preflight.discover_source_streams("cluster-abc", "us-east-1", client=client)


def test_only_creating_stream_raises() -> None:
    client = MagicMock()
    client.list_streams.return_value = {"streams": [_stream("s1", "CREATING")]}
    with pytest.raises(ConfigError, match="CDC is not enabled"):
        preflight.discover_source_streams("cluster-abc", "us-east-1", client=client)


def test_single_active_stream_returns_name() -> None:
    client = MagicMock()
    client.list_streams.return_value = {"streams": [_stream("s1", "ACTIVE")]}
    client.get_stream.return_value = _get_stream_resp("s1", "dsql-cdc-stream")

    out = preflight.discover_source_streams("cluster-abc", "us-east-1", client=client)
    assert out == ("dsql-cdc-stream",)


def test_mixed_statuses_keeps_only_active() -> None:
    client = MagicMock()
    client.list_streams.return_value = {
        "streams": [_stream("s1", "ACTIVE"), _stream("s2", "CREATING")]
    }
    client.get_stream.return_value = _get_stream_resp("s1", "dsql-cdc-stream")

    out = preflight.discover_source_streams("cluster-abc", "us-east-1", client=client)
    assert out == ("dsql-cdc-stream",)
    # get_stream only called for the ACTIVE one
    assert client.get_stream.call_count == 1


def test_multiple_active_streams_returns_all() -> None:
    client = MagicMock()
    client.list_streams.return_value = {
        "streams": [_stream("s1", "ACTIVE"), _stream("s2", "ACTIVE")]
    }
    client.get_stream.side_effect = [
        _get_stream_resp("s1", "stream-one"),
        _get_stream_resp("s2", "stream-two"),
    ]

    out = preflight.discover_source_streams("cluster-abc", "us-east-1", client=client)
    assert out == ("stream-one", "stream-two")


def test_missing_kinesis_target_raises() -> None:
    client = MagicMock()
    client.list_streams.return_value = {"streams": [_stream("s1", "ACTIVE")]}
    client.get_stream.return_value = {
        "clusterIdentifier": "cluster-abc",
        "streamIdentifier": "s1",
        "status": "ACTIVE",
        "targetDefinition": {},  # no kinesis
    }
    with pytest.raises(ConfigError, match="no targetDefinition.kinesis.streamArn"):
        preflight.discover_source_streams("cluster-abc", "us-east-1", client=client)


def test_paginates_list_streams() -> None:
    client = MagicMock()
    client.list_streams.side_effect = [
        {"streams": [_stream("s1", "ACTIVE")], "nextToken": "tok"},
        {"streams": [_stream("s2", "ACTIVE")]},
    ]
    client.get_stream.side_effect = [
        _get_stream_resp("s1", "stream-one"),
        _get_stream_resp("s2", "stream-two"),
    ]

    out = preflight.discover_source_streams("cluster-abc", "us-east-1", client=client)
    assert out == ("stream-one", "stream-two")
    assert client.list_streams.call_count == 2
