from __future__ import annotations

import json
import logging
import os
import sys
from urllib.parse import unquote_plus

import boto3
import botocore.exceptions

from pipeline import settings
from pipeline.sort.rules import NoRuleMatchError, match

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

MAX_MESSAGES = 10
WAIT_SECONDS = 20

def _make_client(service: str):

    inside_ecs = bool(
        os.environ.get("ECS_CONTAINER_METADATA_URI") or
        os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
    )
    has_key = bool(os.environ.get("AWS_ACCESS_KEY_ID"))

    use_profile = (
        None
        if inside_ecs or has_key
        else (settings.AWS_PROFILE or None)
    )
    session = boto3.Session(
        profile_name=use_profile,
        region_name=settings.AWS_REGION,
    )
    return session.client(service)

def _sorted_key(table: str, data_date: str, raw_key: str) -> str:
    filename = raw_key.rsplit("/", 1)[-1]
    return f"{settings.PREFIX_SORTED}/{table}/{data_date}/{filename}"


def copy_to_sorted(s3, raw_key: str, sorted_key: str) -> None:
    s3.copy_object(
        Bucket=settings.BUCKET,
        CopySource={"Bucket": settings.BUCKET, "Key": raw_key},
        Key=sorted_key,
    )

def tag_raw_object(s3, raw_key: str) -> None:
    s3.put_object_tagging(
        Bucket=settings.BUCKET,
        Key=raw_key,
        Tagging={
            "TagSet": [
                {"Key": "sorted", "Value": "true"},
            ]
        },
    )

def send_load_message(sqs, table: str, data_date: str) -> None:
    sqs.send_message(
        QueueUrl=settings.SQS_LOAD_QUEUE_URL,
        MessageBody=json.dumps({
            "table": table,
            "data_date": data_date,
        }),
    )

def parse_s3_key_from_message(body: str) -> str:
    event = json.loads(body)
    record = event["Records"][0]
    raw_key = unquote_plus(record["s3"]["object"]["key"])
    return raw_key

def process_message(s3, sqs, receipt_handle: str, body: str) -> None:

    try:
        raw_key = parse_s3_key_from_message(body)
    except (KeyError, IndexError, ValueError, Exception) as exc:
        logger.error(json.dumps({
            "event": "sort_message_parse_error",
            "error": str(exc),
            "body_preview": body[:200],
        }))
        return 

    try:
        result = match(raw_key)
    except NoRuleMatchError as exc:
        logger.error(json.dumps({
            "event": "sort_rule_not_found",
            "key": raw_key,
            "error": str(exc),
        }))
        return 

    sorted_key = _sorted_key(result.table, result.data_date, raw_key)

    copy_to_sorted(s3, raw_key, sorted_key)

    tag_raw_object(s3, raw_key)

    send_load_message(sqs, result.table, result.data_date)

    sqs.delete_message(
        QueueUrl=settings.SQS_SORT_QUEUE_URL,
        ReceiptHandle=receipt_handle,
    )

    logger.info(json.dumps({
        "event": "object_sorted",
        "table": result.table,
        "data_date": result.data_date,
        "raw_key": raw_key,
        "sorted_key": sorted_key,
        "row_count": None, 
    }))

def run() -> None:
    s3  = _make_client("s3")
    sqs = _make_client("sqs")

    if not settings.SQS_SORT_QUEUE_URL:
        raise EnvironmentError(
            "SQS_SORT_QUEUE_URL is not set. "
            "Run scripts/deploy_sort.py to create the queues."
        )

    if not settings.SQS_LOAD_QUEUE_URL:
        raise EnvironmentError(
            "SQS_LOAD_QUEUE_URL is not set. "
            "Run scripts/deploy_sort.py to create the queues."
        )

    logger.info(json.dumps({
        "event": "sort_worker_started",
        "sort_queue": settings.SQS_SORT_QUEUE_URL,
        "load_queue": settings.SQS_LOAD_QUEUE_URL,
    }))

    while True:
        response = sqs.receive_message(
            QueueUrl=settings.SQS_SORT_QUEUE_URL,
            MaxNumberOfMessages=MAX_MESSAGES,
            WaitTimeSeconds=WAIT_SECONDS,   
            AttributeNames=["ApproximateReceiveCount"],
        )

        messages = response.get("Messages", [])
        for msg in messages:
            process_message(
                s3=s3,
                sqs=sqs,
                receipt_handle=msg["ReceiptHandle"],
                body=msg["Body"],
            )

if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.error(json.dumps({
            "event": "sort_worker_failed",
            "error": str(exc),
            "error_type": type(exc).__name__,
        }))
        sys.exit(1)
