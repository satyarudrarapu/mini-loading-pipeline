from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.load_creds import load, load_config
load()
load_config()

import argparse
import json
import re
import time
from datetime import datetime, timezone

import boto3

from pipeline import settings

KEY_PATTERN = re.compile(
    r"^raw/usgs/all_day/\d{4}-\d{2}-\d{2}/\d{6}\.geojson$"
)


def _verify_s3(s3_key: str, expected_bytes: int) -> None:
    """Check the object exists in S3 with the correct byte count."""
    s3 = boto3.Session(region_name=settings.AWS_REGION).client("s3")
    head = s3.head_object(Bucket=settings.BUCKET, Key=s3_key)
    actual = head["ContentLength"]

    key_ok = bool(KEY_PATTERN.match(s3_key))
    bytes_ok = actual == expected_bytes

    print("\n=== F1 VERIFICATION ===")
    print(f"  Key pattern  : {'PASS' if key_ok   else 'FAIL'}  ({s3_key})")
    print(f"  Bytes match  : {'PASS' if bytes_ok else 'FAIL'}  "
          f"(expected {expected_bytes}, got {actual})")
    print(f"  S3 location  : s3://{settings.BUCKET}/{s3_key}")

    if key_ok and bytes_ok:
        print("\nF1 PASSED.")
    else:
        print("\nF1 FAILED.")
        sys.exit(1)

def run_worker_local() -> None:
   
    print("Mode: --worker  (running worker.run() in-process)\n")
    from pipeline.ingest.worker import run, _build_s3_key, _utc_now

    now = _utc_now()
    s3_key = _build_s3_key(now)

    run()  

    s3 = boto3.Session(region_name=settings.AWS_REGION).client("s3")
    head = s3.head_object(Bucket=settings.BUCKET, Key=s3_key)
    actual = head["ContentLength"]

    _verify_s3(s3_key, actual)

def run_invoke_ecs() -> None:
   
    print("Mode: --invoke  (starting ECS Fargate ingest task directly)\n")

    if not settings.ECS_SUBNET_ID or not settings.ECS_SECURITY_GROUP_ID:
        print("ERROR: ECS_SUBNET_ID or ECS_SECURITY_GROUP_ID not set.")
        print("Run: .venv/Scripts/python.exe scripts/deploy.py first.")
        sys.exit(1)

    ecs = boto3.Session(region_name=settings.AWS_REGION).client("ecs")

    print(f"Starting ECS task: {settings.ECS_INGEST_TASK_DEF}")
    response = ecs.run_task(
        cluster=settings.ECS_CLUSTER_NAME,
        taskDefinition=settings.ECS_INGEST_TASK_DEF,
        launchType="FARGATE",
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": [settings.ECS_SUBNET_ID],
                "securityGroups": [settings.ECS_SECURITY_GROUP_ID],
                "assignPublicIp": "ENABLED",
            }
        },
        overrides={
            "containerOverrides": [{
                "name": settings.ECS_INGEST_CONTAINER_NAME,
                "environment": [
                    {"name": "PIPELINE_BUCKET", "value": settings.BUCKET},
                ],
            }]
        },
    )

    failures = response.get("failures", [])
    if failures:
        print("FAILED: ECS run_task returned failures:")
        print(json.dumps(failures, indent=2))
        sys.exit(1)

    task_arn = response["tasks"][0]["taskArn"]
    task_id  = task_arn.split("/")[-1]
    print(f"ECS task started: {task_id}")

    print("Waiting for ECS task to finish", end="", flush=True)
    for _ in range(60):   
        time.sleep(3)
        print(".", end="", flush=True)
        resp = ecs.describe_tasks(
            cluster=settings.ECS_CLUSTER_NAME,
            tasks=[task_arn],
        )
        if not resp["tasks"]:
            continue
        task = resp["tasks"][0]
        status = task["lastStatus"]
        if status == "STOPPED":
            exit_code = task["containers"][0].get("exitCode", -1)
            print(f"\nECS task stopped. Exit code: {exit_code}")
            if exit_code != 0:
                print("FAILED: ECS task exited with non-zero code.")
               
                print(f"Check logs: CloudWatch → /ecs/mini-loading-pipeline-ingest")
                sys.exit(1)
            break
    else:
        print("\nFAILED: ECS task did not finish within 3 minutes.")
        sys.exit(1)

    print("\nChecking S3 for new file...")
    s3 = boto3.Session(region_name=settings.AWS_REGION).client("s3")
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    prefix = f"{settings.PREFIX_RAW_ALL_DAY}/{today}/"

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=settings.BUCKET, Prefix=prefix)
    objects = sorted(
        [obj for page in pages for obj in page.get("Contents", [])],
        key=lambda o: o["LastModified"],
        reverse=True,
    )

    if not objects:
        print(f"FAILED: No files found under {prefix}")
        sys.exit(1)

    latest = objects[0]
    s3_key = latest["Key"]
    byte_count = latest["Size"]
    print(f"  Latest file: {s3_key} ({byte_count} bytes)")

    _verify_s3(s3_key, byte_count)


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Test F1 — USGS ingest pipeline (EventBridge → ECS)."
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--worker",
        action="store_true",
        help="Run worker.py in-process (tests ingest logic only, fastest)",
    )
    group.add_argument(
        "--invoke",
        action="store_true",
        help="Start real ECS Fargate task directly (simulates EventBridge)",
    )
    args = p.parse_args()

    if args.worker:
        run_worker_local()
    else:
        run_invoke_ecs()
