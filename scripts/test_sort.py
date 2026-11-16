from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.load_creds import load, load_config
load()
load_config()

import boto3
import botocore.exceptions

from pipeline import settings
from pipeline.sort.rules import match, NoRuleMatchError

AWS_CLI = r"C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe"


def _client(service: str):
    return boto3.Session(region_name=settings.AWS_REGION).client(service)

def run_worker_local() -> None:
    print("Starting sort worker (local in-process). Press Ctrl+C to stop.\n")
    from pipeline.sort.worker import run
    try:
        run()
    except KeyboardInterrupt:
        print("\nSort worker stopped.")

def test_f3_to_f6() -> None:
    print("=== F3–F6 TEST ===\n")

    s3  = _client("s3")
    sqs = _client("sqs")

    # 1. Upload a test file to raw/
    now = datetime.now(tz=timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H%M%S")
    test_key = f"raw/usgs/all_day/{date_str}/{time_str}.geojson"
    test_body = json.dumps({"type": "FeatureCollection", "features": [], "_test": True}).encode()

    print(f"[1] Uploading test file: s3://{settings.BUCKET}/{test_key}")
    s3.put_object(
        Bucket=settings.BUCKET,
        Key=test_key,
        Body=test_body,
        ContentType="application/geo+json",
    )
    print("  Uploaded.")

    # 2. Wait for SQS notification (F3)
    print(f"\n[2] Waiting for SQS sort queue message (F3)")
    receipt_handle = None
    message_body = None
    for attempt in range(40):  
        resp = sqs.receive_message(
            QueueUrl=settings.SQS_SORT_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )
        for msg in resp.get("Messages", []):
            try:
                from urllib.parse import unquote_plus
                body = json.loads(msg["Body"])
                key = unquote_plus(body["Records"][0]["s3"]["object"]["key"])
                if key == test_key:
                    receipt_handle = msg["ReceiptHandle"]
                    message_body = msg["Body"]
                    break
            except (KeyError, json.JSONDecodeError):
                continue
        if receipt_handle:
            break
        print(f"  Attempt {attempt+1}/20: message not yet visible, retrying...")

    if not receipt_handle:
        print("FAIL: No SQS message arrived for the test file within timeout.")
        sys.exit(1)
    print("  F3 PASS: SQS message received for test file.")

    # 3. Process the message using the sort worker logic directly
    print(f"\n[3] Processing SQS message through sort worker...")
    from pipeline.sort.worker import process_message
    process_message(s3=s3, sqs=sqs, receipt_handle=receipt_handle, body=message_body)
    print("  Message processed.")

    # 4. Verify sorted/ copy (F4)
    result = match(test_key)
    filename = test_key.rsplit("/", 1)[-1]
    sorted_key = f"{settings.PREFIX_SORTED}/{result.table}/{result.data_date}/{filename}"
    print(f"\n[4] Verifying sorted copy (F4): {sorted_key}")
    try:
        head = s3.head_object(Bucket=settings.BUCKET, Key=sorted_key)
        print(f"  F4 PASS: sorted copy exists ({head['ContentLength']} bytes).")
    except botocore.exceptions.ClientError:
        print(f"  F4 FAIL: sorted copy not found at s3://{settings.BUCKET}/{sorted_key}")
        sys.exit(1)

    # 5. Verify sorted=true tag on raw object (F4)
    print(f"\n[5] Verifying sorted=true tag on raw object (F4)")
    tag_resp = s3.get_object_tagging(Bucket=settings.BUCKET, Key=test_key)
    tags = {t["Key"]: t["Value"] for t in tag_resp.get("TagSet", [])}
    if tags.get("sorted") == "true":
        print("  F4 PASS: raw object has tag sorted=true.")
    else:
        print(f"  F4 FAIL: tag not found. Tags: {tags}")
        sys.exit(1)

    # 6. Verify load queue message (F6)
    # The load queue may have many messages from previous sorts.
    # Poll multiple times to find the matching one.
    print(f"\n[6] Verifying load queue message (F6)")
    time.sleep(2)
    found_load = False
    receipts_to_return = []
    for _ in range(15):   # poll up to 15 times (150 messages)
        load_resp = sqs.receive_message(
            QueueUrl=settings.SQS_LOAD_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
        )
        load_messages = load_resp.get("Messages", [])
        if not load_messages:
            break
        for lmsg in load_messages:
            receipts_to_return.append(lmsg["ReceiptHandle"])
            try:
                lbody = json.loads(lmsg["Body"])
                if lbody.get("table") == result.table and lbody.get("data_date") == result.data_date:
                    found_load = True
            except (KeyError, json.JSONDecodeError):
                pass
        if found_load:
            break

    # Return all received messages so they stay in the queue
    for rh in receipts_to_return:
        try:
            sqs.change_message_visibility(
                QueueUrl=settings.SQS_LOAD_QUEUE_URL,
                ReceiptHandle=rh,
                VisibilityTimeout=0,
            )
        except Exception:
            pass

    if found_load:
        print(f"  F6 PASS: load message found (table={result.table}, data_date={result.data_date}).")
    else:
        print("  F6 FAIL: load message not found in load queue.")
        sys.exit(1)

    print("\n[7] Cleaning up test objects...")
    s3.delete_object(Bucket=settings.BUCKET, Key=test_key)
    s3.delete_object(Bucket=settings.BUCKET, Key=sorted_key)
    print("  Test objects deleted.")

    print("\n=== F3 PASS, F4 PASS, F6 PASS ===")


def test_f5() -> None:

    print("=== F5 TEST (unmatched file) ===\n")

    s3 = _client("s3")
    sqs = _client("sqs")

    test_key = "raw/unknown/test_unmatched_f5.txt"
    test_body = b"test file for F5 - unmatched prefix"

    print(f"[1] Uploading unmatched test file: s3://{settings.BUCKET}/{test_key}")
    s3.put_object(Bucket=settings.BUCKET, Key=test_key, Body=test_body)

    print("[2] Waiting for SQS message...")
    receipt_handle = None
    message_body = None
    for attempt in range(20):
        resp = sqs.receive_message(
            QueueUrl=settings.SQS_SORT_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
        )
        for msg in resp.get("Messages", []):
            try:
                from urllib.parse import unquote_plus
                body = json.loads(msg["Body"])
                key = unquote_plus(body["Records"][0]["s3"]["object"]["key"])
                if key == test_key:
                    receipt_handle = msg["ReceiptHandle"]
                    message_body = msg["Body"]
                    break
            except (KeyError, json.JSONDecodeError):
                continue
        if receipt_handle:
            break
        print(f"  Attempt {attempt+1}/20: not yet visible...")

    if not receipt_handle:
        print("Could not find the SQS message for the unmatched file.")
        s3.delete_object(Bucket=settings.BUCKET, Key=test_key)
        sys.exit(1)

    print(f"[3] Processing message (expect NoRuleMatchError — message must NOT be deleted)")
    from pipeline.sort.worker import process_message
    process_message(s3=s3, sqs=sqs, receipt_handle=receipt_handle, body=message_body)

    print("[4] Verifying message was not deleted (F5)...")
    try:
        sqs.change_message_visibility(
            QueueUrl=settings.SQS_SORT_QUEUE_URL,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=0,
        )
        print("  F5 PASS: message still in sort queue (not deleted after no-rule-match).")
    except sqs.exceptions.ReceiptHandleIsInvalid:
        print("  F5 FAIL: message was deleted even though no rule matched.")
        s3.delete_object(Bucket=settings.BUCKET, Key=test_key)
        sys.exit(1)

    s3.delete_object(Bucket=settings.BUCKET, Key=test_key)
    print("  Test file cleaned up.")
    print("\n=== F5 PASS ===")


def test_n3() -> None:
    print("=== N3 TEST (no silent loss) ===\n")
    from scripts.reconcile import run_reconcile
    n = run_reconcile()
    if n == 0:
        print("\nN3 PASS.")
    else:
        print(f"\nN3 FAIL: {n} object(s) missing.")
        sys.exit(1)

def start_sort_worker_on_ecs() -> None:

    print("Starting sort worker as ECS Fargate task...\n")

    if not settings.ECS_SUBNET_ID or not settings.ECS_SECURITY_GROUP_ID:
        print("ERROR: ECS_SUBNET_ID or ECS_SECURITY_GROUP_ID not set.")
        print("Run: .venv/Scripts/python.exe scripts/deploy_sort.py first.")
        sys.exit(1)

    if not settings.SQS_SORT_QUEUE_URL or not settings.SQS_LOAD_QUEUE_URL:
        print("ERROR: SQS queue URLs not set.")
        print("Run: .venv/Scripts/python.exe scripts/deploy_sort.py first.")
        sys.exit(1)

    ecs = _client("ecs")

    response = ecs.run_task(
        cluster=settings.ECS_CLUSTER_NAME,
        taskDefinition=settings.ECS_SORT_TASK_DEF,
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
                "name": settings.ECS_SORT_CONTAINER_NAME,
                "environment": [
                    {"name": "PIPELINE_BUCKET", "value": settings.BUCKET},
                    {"name": "SQS_SORT_QUEUE_URL", "value": settings.SQS_SORT_QUEUE_URL},
                    {"name": "SQS_LOAD_QUEUE_URL", "value": settings.SQS_LOAD_QUEUE_URL},
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

    print(f"Sort worker ECS task started.")
    print(f"  Task ID  : {task_id}")
    print(f"  Task ARN : {task_arn}")
    print(f"  Cluster  : {settings.ECS_CLUSTER_NAME}")
    print(f"  Task def : {settings.ECS_SORT_TASK_DEF}")
    print()
    print("The sort worker is now running in AWS and polling the sort queue.")
    print("It will keep running until you stop it.")
   

   
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Milestone 3 tests (F3–F6, N2, N3).")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--run-worker",
        action="store_true",
        help="Start sort worker locally on your laptop (Ctrl+C to stop)",
    )
    group.add_argument(
        "--start-ecs",
        action="store_true",
        help="Start sort worker as a real ECS Fargate task in AWS",
    )
    group.add_argument(
        "--test-f3-f6",
        action="store_true",
        help="Upload test file, verify sort and load queue (F3, F4, F6)",
    )
    group.add_argument(
        "--test-f5",
        action="store_true",
        help="Upload unmatched file, verify message stays in queue (F5)",
    )
    group.add_argument(
        "--test-n3",
        action="store_true",
        help="Run reconcile script, verify 0 missing objects (N3)",
    )
    args = p.parse_args()

    if args.run_worker:
        run_worker_local()
    elif args.start_ecs:
        start_sort_worker_on_ecs()
    elif args.test_f3_f6:
        test_f3_to_f6()
    elif args.test_f5:
        test_f5()
    else:
        test_n3()
