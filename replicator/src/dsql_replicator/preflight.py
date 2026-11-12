"""Source-cluster preflight: verify CDC is enabled on a given DSQL cluster and
derive the Kinesis stream name(s) it emits to, so the operator only has to pass
`source_cluster_id` and never has to look up the stream ARN by hand.

Behavior:
  - List all streams attached to the cluster.
  - Keep the ones in state `ACTIVE` — anything in CREATING / DELETING / FAILED
    is not usable, and we fail fast rather than consume from a half-provisioned
    stream.
  - For each kept stream, read `targetDefinition.kinesis.streamArn` and parse
    the stream name out of the ARN (`arn:aws:kinesis:<region>:<account>:stream/<name>`).
  - Raise `ConfigError` if the cluster has zero ACTIVE streams — that is the
    "CDC not enabled" signal.

Separate from `config.py` so the pure env-parsing path stays side-effect-free
and so this one boto3 call is trivially mockable in unit tests.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .errors import ConfigError

if TYPE_CHECKING:
    from mypy_boto3_dsql.client import DSQLClient


def _stream_name_from_arn(arn: str) -> str:
    # arn:aws:kinesis:<region>:<account>:stream/<name>
    marker = ":stream/"
    idx = arn.find(marker)
    if idx < 0:
        raise ConfigError(f"unexpected Kinesis stream ARN format: {arn!r}")
    return arn[idx + len(marker) :]


def discover_source_streams(
    cluster_id: str,
    region: str,
    *,
    client: DSQLClient | None = None,
) -> tuple[str, ...]:
    """Return the Kinesis stream names the given DSQL cluster emits CDC to.

    Raises ConfigError if CDC is not enabled (no ACTIVE streams).
    """
    if client is None:
        import boto3

        client = boto3.client("dsql", region_name=region)

    streams: list[dict] = []
    next_token: str | None = None
    while True:
        kwargs: dict = {"clusterIdentifier": cluster_id}
        if next_token:
            kwargs["nextToken"] = next_token
        try:
            resp = client.list_streams(**kwargs)
        except AttributeError as exc:
            raise ConfigError(
                "DSQL CDC APIs (ListStreams/GetStream) are not available in this "
                "boto3 install — public boto3 does not yet ship the CDC operations. "
                "Either (a) pass SOURCE_STREAMS directly and unset SOURCE_CLUSTER_ID "
                "to skip preflight, or (b) drop the preview DSQL service model into "
                "replicator/aws-models/dsql/2018-05-10/service-2.json so the "
                "container's AWS_DATA_PATH override picks it up."
            ) from exc
        streams.extend(resp.get("streams", []))
        next_token = resp.get("nextToken")
        if not next_token:
            break

    active = [s for s in streams if s.get("status") == "ACTIVE"]
    if not active:
        statuses = sorted({s.get("status", "UNKNOWN") for s in streams})
        raise ConfigError(
            f"CDC is not enabled on DSQL cluster {cluster_id!r} in {region}: "
            f"found {len(streams)} stream(s), none ACTIVE (statuses: {statuses or 'none'}). "
            "Enable CDC on the cluster before running the replicator."
        )

    names: list[str] = []
    for s in active:
        detail = client.get_stream(
            clusterIdentifier=cluster_id,
            streamIdentifier=s["streamIdentifier"],
        )
        kinesis = detail.get("targetDefinition", {}).get("kinesis", {})
        arn = kinesis.get("streamArn")
        if not arn:
            raise ConfigError(
                f"stream {s['streamIdentifier']!r} on cluster {cluster_id!r} "
                "has no targetDefinition.kinesis.streamArn — only Kinesis CDC targets are supported."
            )
        names.append(_stream_name_from_arn(arn))

    return tuple(names)
