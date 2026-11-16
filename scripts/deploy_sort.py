from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.load_creds import load
load()

import boto3
import botocore.exceptions

from pipeline import settings

ROOT = Path(__file__).resolve().parent.parent
AWS_CLI = r"C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe"
REGION = settings.AWS_REGION
ACCOUNT_ID = settings.AWS_ACCOUNT_ID
BUCKET = settings.BUCKET
CLUSTER_NAME = settings.ECS_CLUSTER_NAME
TASK_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/mini-loading-pipeline-ecs-task-role"
EXEC_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/mini-loading-pipeline-ecs-execution-role"
ECR_IMAGE_URI = (
    f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com"
    "/mini-loading-pipeline-ingest:latest"
)
LOG_GROUP_SORT = "/ecs/mini-loading-pipeline-sort"
SORT_QUEUE_NAME  = "mini-loading-pipeline-sort"
SORT_DLQ_NAME = "mini-loading-pipeline-sort-dlq"
LOAD_QUEUE_NAME  = "mini-loading-pipeline-load"
TASK_DEF_FAMILY  = "mini-loading-pipeline-sort-task"


def c(service: str):
    return boto3.Session(region_name=REGION).client(service)

def create_sqs_queues() -> tuple[str, str, str]:

    print("\n[1] SQS queues")
    sqs = c("sqs")

    try:
        resp = sqs.get_queue_url(QueueName=SORT_DLQ_NAME)
        sort_dlq_url = resp["QueueUrl"]
        print(f"  Sort DLQ exists: {sort_dlq_url}")
    except sqs.exceptions.QueueDoesNotExist:
        resp = sqs.create_queue(
            QueueName=SORT_DLQ_NAME,
            Attributes={
                "MessageRetentionPeriod": str(14 * 24 * 3600), 
            },
        )
        sort_dlq_url = resp["QueueUrl"]
        print(f"  Sort DLQ created: {sort_dlq_url}")
        time.sleep(2) 

    dlq_attrs = sqs.get_queue_attributes(
        QueueUrl=sort_dlq_url,
        AttributeNames=["QueueArn"],
    )
    sort_dlq_arn = dlq_attrs["Attributes"]["QueueArn"]

    try:
        resp = sqs.get_queue_url(QueueName=SORT_QUEUE_NAME)
        sort_queue_url = resp["QueueUrl"]
        print(f"  Sort queue exists: {sort_queue_url}")
    except sqs.exceptions.QueueDoesNotExist:
        resp = sqs.create_queue(
            QueueName=SORT_QUEUE_NAME,
            Attributes={
                "VisibilityTimeout": "60",
                "MessageRetentionPeriod": str(14 * 24 * 3600),
                "ReceiveMessageWaitTimeSeconds": "20",
                "RedrivePolicy": json.dumps({
                    "maxReceiveCount": "5",
                    "deadLetterTargetArn": sort_dlq_arn,
                }),
            },
        )
        sort_queue_url = resp["QueueUrl"]
        print(f"  Sort queue created: {sort_queue_url}")
        time.sleep(2)

    try:
        resp = sqs.get_queue_url(QueueName=LOAD_QUEUE_NAME)
        load_queue_url = resp["QueueUrl"]
        print(f"  Load queue exists: {load_queue_url}")
    except sqs.exceptions.QueueDoesNotExist:
        resp = sqs.create_queue(
            QueueName=LOAD_QUEUE_NAME,
            Attributes={
                "VisibilityTimeout": "300",   
                "MessageRetentionPeriod": str(14 * 24 * 3600),
                "ReceiveMessageWaitTimeSeconds": "20",
            },
        )
        load_queue_url = resp["QueueUrl"]
        print(f"  Load queue created: {load_queue_url}")
        time.sleep(2)

    return sort_queue_url, sort_dlq_url, load_queue_url


def allow_s3_to_publish_sqs(sort_queue_url: str) -> None:

    print("\n[2] SQS queue policy (allow S3 to publish)")
    sqs = c("sqs")

    attrs = sqs.get_queue_attributes(
        QueueUrl=sort_queue_url,
        AttributeNames=["QueueArn"],
    )
    sort_queue_arn = attrs["Attributes"]["QueueArn"]

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowS3ToPublishToSortQueue",
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "SQS:SendMessage",
                "Resource": sort_queue_arn,
                "Condition": {
                    "ArnLike": {
                        "aws:SourceArn": f"arn:aws:s3:::{BUCKET}"
                    }
                },
            }
        ],
    }

    sqs.set_queue_attributes(
        QueueUrl=sort_queue_url,
        Attributes={"Policy": json.dumps(policy)},
    )
    print(f"  Policy set: S3 bucket {BUCKET} can publish to sort queue")
    return sort_queue_arn


def configure_s3_notification(sort_queue_arn: str) -> None:

    print("\n[3] S3 event notification (raw/ → sort queue)")
    s3 = c("s3")

    notification_config = {
        "QueueConfigurations": [
            {
                "Id": "RawObjectCreatedToSortQueue",
                "QueueArn": sort_queue_arn,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {"Name": "prefix", "Value": "raw/"}
                        ]
                    }
                },
            }
        ]
    }

    s3.put_bucket_notification_configuration(
        Bucket=BUCKET,
        NotificationConfiguration=notification_config,
    )
    print(f"  Notification configured: s3://{BUCKET}/raw/* → sort queue")


def create_sort_log_group() -> None:
    """Create CloudWatch log group for sort worker logs ."""
    print("\n[4] CloudWatch log group for sort worker")
    logs = c("logs")
    try:
        logs.create_log_group(logGroupName=LOG_GROUP_SORT)
        print(f"  Created: {LOG_GROUP_SORT}")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print(f"  Already exists: {LOG_GROUP_SORT}")
        else:
            raise


def register_sort_task_definition(
    sort_queue_url: str,
    load_queue_url: str,
) -> str:
    print("\n[5] ECS sort task definition")
    ecs = c("ecs")

    response = ecs.register_task_definition(
        family=TASK_DEF_FAMILY,
        taskRoleArn=TASK_ROLE_ARN,
        executionRoleArn=EXEC_ROLE_ARN,
        networkMode="awsvpc",
        requiresCompatibilities=["FARGATE"],
        cpu="256",
        memory="512",
        containerDefinitions=[
            {
                "name": settings.ECS_SORT_CONTAINER_NAME,
                "image": ECR_IMAGE_URI,
                "command": ["python", "-m", "pipeline.sort.worker"],
                "essential": True,
                "environment": [
                    {"name": "PIPELINE_BUCKET", "value": BUCKET},
                    {"name": "AWS_REGION", "value": REGION},
                    {"name": "SQS_SORT_QUEUE_URL", "value": sort_queue_url},
                    {"name": "SQS_LOAD_QUEUE_URL", "value": load_queue_url},
                ],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": LOG_GROUP_SORT,
                        "awslogs-region": REGION,
                        "awslogs-stream-prefix": "sort",
                    },
                },
            }
        ],
    )

    revision = response["taskDefinition"]["revision"]
    print(f"  Registered: {TASK_DEF_FAMILY}:{revision}")
    return f"{TASK_DEF_FAMILY}:{revision}"


def allow_ecs_task_role_on_queues(
    sort_queue_url: str,
    sort_dlq_url: str,
    load_queue_url: str,
) -> None:

    print("\n[6] IAM inline policy for ECS task role (SQS access)")
    iam = c("iam")

    sqs = c("sqs")
    sort_attrs = sqs.get_queue_attributes(QueueUrl=sort_queue_url, AttributeNames=["QueueArn"])
    dlq_attrs = sqs.get_queue_attributes(QueueUrl=sort_dlq_url,  AttributeNames=["QueueArn"])
    load_attrs = sqs.get_queue_attributes(QueueUrl=load_queue_url, AttributeNames=["QueueArn"])

    sort_queue_arn = sort_attrs["Attributes"]["QueueArn"]
    sort_dlq_arn = dlq_attrs["Attributes"]["QueueArn"]
    load_queue_arn = load_attrs["Attributes"]["QueueArn"]

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "SortWorkerSQS",
                "Effect": "Allow",
                "Action": [
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                ],
                "Resource": [sort_queue_arn, sort_dlq_arn],
            },
            {
                "Sid": "LoadQueueSend",
                "Effect": "Allow",
                "Action": ["sqs:SendMessage", "sqs:GetQueueAttributes"],
                "Resource": load_queue_arn,
            },
        ],
    }

    role_name = "mini-loading-pipeline-ecs-task-role"
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="mini-loading-pipeline-sqs-access",
        PolicyDocument=json.dumps(policy),
    )
    print(f"  SQS inline policy attached to {role_name}")


def store_sort_config(
    sort_queue_url: str,
    sort_dlq_url: str,
    load_queue_url: str,
    sort_task_def: str,
) -> None:
   
    print("\n[7] Verifying sort config is consistent")
    ecs = c("ecs")

    try:
        td = ecs.describe_task_definition(taskDefinition=sort_task_def)
        containers = td["taskDefinition"]["containerDefinitions"]
        env = {e["name"]: e["value"] for c2 in containers for e in c2.get("environment", [])}
        sort_q = env.get("SQS_SORT_QUEUE_URL", "")
        load_q = env.get("SQS_LOAD_QUEUE_URL", "")
        if sort_q and load_q:
            print(f" Sort task def {sort_task_def} has queue URLs: OK")
        else:
            print(f" Warning: sort task def missing queue URLs in env")
    except Exception as e:
        print(f" Warning: could not verify sort task def: {e}")


def create_dlq_alarm() -> None:
   
    print("\n[8] CloudWatch DLQ alarm (N2)")
    cw = c("cloudwatch")
    alarm_name = "mini-loading-pipeline-sort-dlq-alarm"

    try:
        cw.put_metric_alarm(
            AlarmName=alarm_name,
            AlarmDescription=(
                "Fires when the sort DLQ holds a message (N2 — fail loud). "
                "Check /ecs/mini-loading-pipeline-sort CloudWatch logs."
            ),
            MetricName="ApproximateNumberOfMessagesVisible",
            Namespace="AWS/SQS",
            Statistic="Sum",
            Dimensions=[
                {"Name": "QueueName", "Value": SORT_DLQ_NAME},
            ],
            Period=60,
            EvaluationPeriods=1,
            Threshold=1,
            ComparisonOperator="GreaterThanOrEqualToThreshold",
            TreatMissingData="notBreaching",
        )
        print(f"  Alarm created: {alarm_name}")
    except Exception as e:
        print(f"  Warning: could not create alarm: {e}")



def main() -> None:
    print("=" * 60)
    print("Mini Loading Pipeline — Milestone 3 Deployment")
    print("Stage: Queue and Sort")
    print("=" * 60)

    sort_queue_url, sort_dlq_url, load_queue_url = create_sqs_queues()
    sort_queue_arn = allow_s3_to_publish_sqs(sort_queue_url)
    configure_s3_notification(sort_queue_arn)
    create_sort_log_group()
    sort_task_def = register_sort_task_definition(sort_queue_url, load_queue_url)
    allow_ecs_task_role_on_queues(sort_queue_url, sort_dlq_url, load_queue_url)
    store_sort_config(sort_queue_url, sort_dlq_url, load_queue_url, sort_task_def)
    create_dlq_alarm()

    print("\n" + "=" * 60)
    print("Milestone 3 deployment complete.")
    print(f"  Sort queue  : {sort_queue_url}")
    print(f"  Sort DLQ    : {sort_dlq_url}")
    print(f"  Load queue  : {load_queue_url}")
    print(f"  Sort task   : {sort_task_def}")
    print(f"  DLQ alarm   : mini-loading-pipeline-sort-dlq-alarm")


if __name__ == "__main__":
    main()
