import os

# The one S3 bucket used by the whole pipeline.
BUCKET: str = os.environ.get(
    "PIPELINE_BUCKET",
    "mini-loading-pipeline-bucket",
)

# S3 key prefixes — raw landing zone.
PREFIX_RAW_ALL_DAY: str = "raw/usgs/all_day"   
PREFIX_RAW_HISTORY: str = "raw/usgs/history"   

# S3 key prefix — sorted zone
PREFIX_SORTED: str = "sorted"

# Live feed: last 24 hours, all magnitudes, GeoJSON format.
USGS_ALL_DAY_URL: str = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
)

# Historical query endpoint (FDSN event API).
USGS_HISTORY_URL: str = (
    "https://earthquake.usgs.gov/fdsnws/event/1/query"
)

# How long (seconds) to wait for the USGS API before giving up.
USGS_REQUEST_TIMEOUT: int = int(os.environ.get("USGS_REQUEST_TIMEOUT", "30"))

# AWS region for all SDK calls.
AWS_REGION: str = os.environ.get("AWS_REGION", "ap-southeast-2")

# AWS CLI profile used for local development.
AWS_PROFILE: str = os.environ.get("AWS_PROFILE", "default")

# AWS account ID needed to build ECR image URIs.
AWS_ACCOUNT_ID: str = os.environ.get("AWS_ACCOUNT_ID", "106217160836")

# ECS cluster that runs all pipeline Fargate tasks.
ECS_CLUSTER_NAME: str = os.environ.get(
    "ECS_CLUSTER_NAME",
    "mini-loading-pipeline-cluster",
)

# Task definition for the ingest worker container.
ECS_INGEST_TASK_DEF: str = os.environ.get(
    "ECS_INGEST_TASK_DEF",
    "mini-loading-pipeline-ingest-task",
)

# Container name inside the task definition.
ECS_INGEST_CONTAINER_NAME: str = os.environ.get(
    "ECS_INGEST_CONTAINER_NAME",
    "ingest-worker",
)

# VPC subnet for the Fargate task.
ECS_SUBNET_ID: str = os.environ.get("ECS_SUBNET_ID", "")

# Security group for the Fargate task.
ECS_SECURITY_GROUP_ID: str = os.environ.get("ECS_SECURITY_GROUP_ID", "")

# IAM role ARN that the ECS task container uses to access S3 and CloudWatch.
ECS_TASK_ROLE_ARN: str = os.environ.get("ECS_TASK_ROLE_ARN", "")

# IAM role ARN that ECS uses to pull the ECR image and create log streams.
ECS_EXECUTION_ROLE_ARN: str = os.environ.get("ECS_EXECUTION_ROLE_ARN", "")

# IAM role ARN that EventBridge uses to start ECS tasks on our behalf.
EVENTBRIDGE_ROLE_ARN: str = os.environ.get("EVENTBRIDGE_ROLE_ARN", "")

# ECR repository URI for the ingest worker image.
ECR_INGEST_REPO_URI: str = os.environ.get(
    "ECR_INGEST_REPO_URI",
    f"{AWS_ACCOUNT_ID}.dkr.ecr.{AWS_REGION}.amazonaws.com/mini-loading-pipeline-ingest",
)

# ---------------------------------------------------------------------------
# Lambda — NOT used for ingest (senior's change: EventBridge → ECS directly).
# Lambda is reserved for Milestone 4 (EMR triggering from load queue).
# ---------------------------------------------------------------------------

LAMBDA_LOAD_TRIGGER_ARN: str = os.environ.get(
    "LAMBDA_LOAD_TRIGGER_ARN",
    "",   # filled in Milestone 4
)


# Sort queue receives one message per raw S3 object (F3).
SQS_SORT_QUEUE_URL: str = os.environ.get("SQS_SORT_QUEUE_URL", "")

# Sort DLQ receives messages that failed 5 times in the sort queue (F5).
SQS_SORT_DLQ_URL: str = os.environ.get("SQS_SORT_DLQ_URL", "")

# Load queue — receives one message per sorted object (F6).
SQS_LOAD_QUEUE_URL: str = os.environ.get("SQS_LOAD_QUEUE_URL", "")

# ECS task definition for the sort worker container.
ECS_SORT_TASK_DEF: str = os.environ.get(
    "ECS_SORT_TASK_DEF",
    "mini-loading-pipeline-sort-task",
)

# Container name inside the sort task definition.
ECS_SORT_CONTAINER_NAME: str = os.environ.get(
    "ECS_SORT_CONTAINER_NAME",
    "sort-worker",
)

EMR_APPLICATION_ID: str = os.environ.get("EMR_APPLICATION_ID", "")
EMR_EXECUTION_ROLE_ARN: str = os.environ.get("EMR_EXECUTION_ROLE_ARN", "")
