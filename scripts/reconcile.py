from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.load_creds import load, load_config
load()
load_config()

import boto3
import botocore.exceptions

from pipeline import settings


def _make_client(service: str):
    return boto3.Session(region_name=settings.AWS_REGION).client(service)


def get_dlq_message_keys(sqs) -> set[str]:

    if not settings.SQS_SORT_DLQ_URL:
        return set()

    keys: set[str] = set()
    seen_message_ids: set[str] = set()

    for _ in range(20):
        resp = sqs.receive_message(
            QueueUrl=settings.SQS_SORT_DLQ_URL,
            MaxNumberOfMessages=10,
            VisibilityTimeout=0, 
            WaitTimeSeconds=1,
        )
        messages = resp.get("Messages", [])
        if not messages:
            break
        for msg in messages:
            if msg["MessageId"] in seen_message_ids:
                continue
            seen_message_ids.add(msg["MessageId"])
            try:
                from urllib.parse import unquote_plus
                body = json.loads(msg["Body"])
                key = unquote_plus(body["Records"][0]["s3"]["object"]["key"])
                keys.add(key)
            except (KeyError, json.JSONDecodeError):
                pass 

    return keys


def run_reconcile() -> int:

    s3 = _make_client("s3")
    sqs = _make_client("sqs")

    print("Checking raw objects for missing sorted tag or DLQ message...")

    dlq_keys = get_dlq_message_keys(sqs)

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=settings.BUCKET, Prefix="raw/")

    missing: list[str] = []
    total = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            total += 1

            try:
                tag_resp = s3.get_object_tagging(
                    Bucket=settings.BUCKET, Key=key
                )
                tags = {
                    t["Key"]: t["Value"]
                    for t in tag_resp.get("TagSet", [])
                }
                if tags.get("sorted") == "true":
                    continue   # sorted successfully
            except botocore.exceptions.ClientError:
                pass

            if key in dlq_keys:
                continue   
            missing.append(key)
            print(f"  MISSING: {key}")

    print()
    print(f"Raw objects checked : {total}")
    print(f"Missing (not sorted, not in DLQ) : {len(missing)}")

    if missing:
        print(f"\nN3 FAILED: {len(missing)} object(s) may be silently lost.")
    else:
        print("\nN3 PASSED: no silent loss detected.")

    return len(missing)

if __name__ == "__main__":
    n = run_reconcile()
    sys.exit(1 if n > 0 else 0)
