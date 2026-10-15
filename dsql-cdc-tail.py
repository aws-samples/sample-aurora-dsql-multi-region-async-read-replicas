#!/usr/bin/env python3
"""Tail DSQL CDC events from Kinesis. Prints each record's JSON payload as it arrives.

Usage:
    ./dsql-cdc-tail.py                       # tail LATEST on dsql-cdc-stream / us-east-1
    ./dsql-cdc-tail.py --from-start          # replay from TRIM_HORIZON
    ./dsql-cdc-tail.py --stream NAME --region us-east-1

Note: if AWS_ACCESS_KEY_ID is set in your shell to a stale value, run with:
    env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN ./dsql-cdc-tail.py
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone

import boto3


def tail(stream_name: str, region: str, iterator_type: str) -> None:
    kinesis = boto3.client("kinesis", region_name=region)

    desc = kinesis.describe_stream(StreamName=stream_name)["StreamDescription"]
    shards = [s["ShardId"] for s in desc["Shards"]]
    print(f"[tail] stream={stream_name} region={region} shards={len(shards)} iter={iterator_type}", file=sys.stderr)

    iterators = {
        shard_id: kinesis.get_shard_iterator(
            StreamName=stream_name, ShardId=shard_id, ShardIteratorType=iterator_type
        )["ShardIterator"]
        for shard_id in shards
    }

    seq = 0
    while True:
        any_records = False
        for shard_id, it in list(iterators.items()):
            if it is None:
                continue
            try:
                resp = kinesis.get_records(ShardIterator=it, Limit=100)
            except kinesis.exceptions.ExpiredIteratorException:
                iterators[shard_id] = kinesis.get_shard_iterator(
                    StreamName=stream_name, ShardId=shard_id, ShardIteratorType="LATEST"
                )["ShardIterator"]
                continue

            iterators[shard_id] = resp.get("NextShardIterator")
            for rec in resp.get("Records", []):
                any_records = True
                seq += 1
                ts = rec["ApproximateArrivalTimestamp"].astimezone(timezone.utc).isoformat()
                data = rec["Data"]
                try:
                    payload = json.loads(data)
                    pretty = json.dumps(payload, indent=2, default=str)
                except json.JSONDecodeError:
                    pretty = repr(data)
                print(f"\n--- #{seq}  {ts}  partition={rec['PartitionKey']}  seq={rec['SequenceNumber']} ---")
                print(pretty)
                sys.stdout.flush()

        if not any_records:
            time.sleep(1.0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", default="dsql-cdc-stream")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--from-start", action="store_true", help="Replay from TRIM_HORIZON instead of LATEST")
    args = ap.parse_args()

    iterator_type = "TRIM_HORIZON" if args.from_start else "LATEST"
    try:
        tail(args.stream, args.region, iterator_type)
    except KeyboardInterrupt:
        print("\n[tail] stopped", file=sys.stderr)


if __name__ == "__main__":
    main()
