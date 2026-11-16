from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.load_creds import load, load_config
load()
load_config()

import boto3
import botocore.exceptions

from pipeline import settings

ROOT = Path(__file__).resolve().parent.parent
AWS_CLI = r"C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe"
REGION = settings.AWS_REGION
ACCOUNT_ID = settings.AWS_ACCOUNT_ID
ECR_REPO_NAME = "mini-loading-pipeline-ingest"
ECR_URI_BASE = f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com"
ECR_URI = f"{ECR_URI_BASE}/{ECR_REPO_NAME}"
LOG_GROUP_ECS = "/ecs/mini-loading-pipeline-ingest"
TASK_ROLE_NAME = "mini-loading-pipeline-ecs-task-role"
EXEC_ROLE_NAME = "mini-loading-pipeline-ecs-execution-role"
EVENTBRIDGE_ROLE_NAME  = "mini-loading-pipeline-eventbridge-role"
TASK_DEF_FAMILY = "mini-loading-pipeline-ingest-task"
EB_RULE_NAME = "mini-loading-pipeline-ingest-schedule"


def c(svc: str):
    """Create a boto3 client using env credentials."""
    return boto3.Session(region_name=REGION).client(svc)

def create_ecr_repo() -> str:
    """Create ECR repo if it does not exist. Return the repo URI."""
    print("\n[1] ECR repository")
    ecr = c("ecr")
    try:
        resp = ecr.describe_repositories(repositoryNames=[ECR_REPO_NAME])
        uri = resp["repositories"][0]["repositoryUri"]
        print(f"  Already exists: {uri}")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "RepositoryNotFoundException":
            resp = ecr.create_repository(
                repositoryName=ECR_REPO_NAME,
                imageScanningConfiguration={"scanOnPush": True},
            )
            uri = resp["repository"]["repositoryUri"]
            print(f"  Created: {uri}")
        else:
            raise
    return uri

def build_and_push_image() -> str:
    """Build Docker image, push to ECR. Return image URI with tag."""
    print("\n[2] Build Docker image")
    image_tag = f"{ECR_URI}:latest"

    r = subprocess.run(["docker", "info"], capture_output=True)
    if r.returncode != 0:
        print("ERROR: Docker is not running. Start Docker Desktop and retry.")
        sys.exit(1)

    result = subprocess.run(
        ["docker", "build", "-t", image_tag, str(ROOT)],
        capture_output=True,
    )
    if result.returncode != 0:
        print("Build failed:", result.stderr.decode("utf-8", errors="replace")[-500:])
        sys.exit(1)
    print(f"  Image built: {image_tag}")

    print("\n[3] Push image to ECR")
    _ecr_inject_credentials()
    subprocess.run(["docker", "context", "use", "default"], capture_output=True)

    push = subprocess.run(["docker", "push", image_tag], capture_output=True)
    if push.returncode != 0:
        print("Push failed:", push.stderr.decode("utf-8", errors="replace")[-500:])
        sys.exit(1)
    print(f"  Pushed: {image_tag}")
    return image_tag


def _ecr_inject_credentials() -> None:
    """Inject ECR auth directly into ~/.docker/config.json (Windows workaround)."""
    result = subprocess.run(
        [AWS_CLI, "ecr", "get-login-password",
         "--region", REGION, "--profile", "default"],
        capture_output=True,
    )
    if result.returncode != 0:
        print("Failed to get ECR password")
        sys.exit(1)
    ecr_password = result.stdout.strip()
    auth_token = base64.b64encode(b"AWS:" + ecr_password).decode("utf-8")
    config_path = Path.home() / ".docker" / "config.json"
    raw = config_path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    cfg = json.loads(raw)
    cfg.pop("credsStore", None)
    cfg.setdefault("auths", {})[ECR_URI_BASE] = {"auth": auth_token}
    config_path.write_bytes(json.dumps(cfg, indent=2).encode("utf-8"))
    print(f"  ECR credentials injected for {ECR_URI_BASE}")


def create_ecs_cluster() -> None:
    print("\n[4] ECS cluster")
    ecs = c("ecs")
    clusters = ecs.describe_clusters(clusters=[settings.ECS_CLUSTER_NAME])
    active = [x for x in clusters["clusters"] if x["status"] == "ACTIVE"]
    if active:
        print(f"  Already exists: {settings.ECS_CLUSTER_NAME}")
        return
    ecs.create_cluster(
        clusterName=settings.ECS_CLUSTER_NAME,
        settings=[{"name": "containerInsights", "value": "disabled"}],
    )
    print(f"  Created: {settings.ECS_CLUSTER_NAME}")


def create_log_group() -> None:
    print("\n[5] CloudWatch log group")
    logs = c("logs")
    try:
        logs.create_log_group(logGroupName=LOG_GROUP_ECS)
        print(f"  Created: {LOG_GROUP_ECS}")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print(f"  Already exists: {LOG_GROUP_ECS}")
        else:
            raise


def _create_role(name: str, trust: dict, policies: list[str]) -> str:
    iam = c("iam")
    try:
        resp = iam.get_role(RoleName=name)
        arn = resp["Role"]["Arn"]
        print(f"  Role exists: {name}")
    except botocore.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchEntityException", "NoSuchEntity"):
            resp = iam.create_role(
                RoleName=name,
                AssumeRolePolicyDocument=json.dumps(trust),
            )
            arn = resp["Role"]["Arn"]
            print(f"  Created role: {name}")
            time.sleep(5)
        else:
            raise
    for policy_arn in policies:
        iam.attach_role_policy(RoleName=name, PolicyArn=policy_arn)
    return arn


def create_iam_roles() -> tuple[str, str]:
    print("\n[6] IAM roles for ECS")
    ecs_trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "ecs-tasks.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    task_role_arn = _create_role(
        name=TASK_ROLE_NAME,
        trust=ecs_trust,
        policies=[
            f"arn:aws:iam::{ACCOUNT_ID}:policy/MiniLoadingPipeline-S3-Access",
            "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
        ],
    )
    exec_role_arn = _create_role(
        name=EXEC_ROLE_NAME,
        trust=ecs_trust,
        policies=[
            "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
        ],
    )
    return task_role_arn, exec_role_arn


def register_task_definition(
    image_uri: str,
    task_role_arn: str,
    exec_role_arn: str,
) -> str:
    print("\n[7] ECS task definition")
    ecs = c("ecs")
    response = ecs.register_task_definition(
        family=TASK_DEF_FAMILY,
        taskRoleArn=task_role_arn,
        executionRoleArn=exec_role_arn,
        networkMode="awsvpc",
        requiresCompatibilities=["FARGATE"],
        cpu="256",
        memory="512",
        containerDefinitions=[{
            "name": settings.ECS_INGEST_CONTAINER_NAME,
            "image": image_uri,
            "essential": True,
            "environment": [
                {"name": "PIPELINE_BUCKET", "value": settings.BUCKET},
                {"name": "AWS_REGION", "value": REGION},
            ],
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": LOG_GROUP_ECS,
                    "awslogs-region": REGION,
                    "awslogs-stream-prefix": "ingest",
                },
            },
        }],
    )
    rev = response["taskDefinition"]["revision"]
    task_def_arn = response["taskDefinition"]["taskDefinitionArn"]
    print(f"  Registered: {TASK_DEF_FAMILY}:{rev}")
    return task_def_arn


def get_default_vpc_config() -> tuple[str, str]:
    print("\n[8] VPC configuration")
    ec2 = c("ec2")
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
    if not vpcs["Vpcs"]:
        print("ERROR: No default VPC found.")
        sys.exit(1)
    vpc_id = vpcs["Vpcs"][0]["VpcId"]
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    subnet_id = subnets["Subnets"][0]["SubnetId"]
    sgs = ec2.describe_security_groups(Filters=[
        {"Name": "vpc-id", "Values": [vpc_id]},
        {"Name": "group-name", "Values": ["default"]},
    ])
    sg_id = sgs["SecurityGroups"][0]["GroupId"]
    print(f"  VPC: {vpc_id}  Subnet: {subnet_id}  SG: {sg_id}")
    return subnet_id, sg_id

def create_eventbridge_role(task_role_arn: str, exec_role_arn: str) -> str:
    
    print("\n[9] IAM role for EventBridge → ECS")
    iam = c("iam")
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "scheduler.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }

    try:
        resp = iam.get_role(RoleName=EVENTBRIDGE_ROLE_NAME)
        eb_role_arn = resp["Role"]["Arn"]
        print(f"  Role exists: {EVENTBRIDGE_ROLE_NAME}")
    except botocore.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchEntityException", "NoSuchEntity"):
            resp = iam.create_role(
                RoleName=EVENTBRIDGE_ROLE_NAME,
                AssumeRolePolicyDocument=json.dumps(trust),
                Description="Allows EventBridge Scheduler to run ECS Fargate tasks",
            )
            eb_role_arn = resp["Role"]["Arn"]
            print(f"  Created role: {EVENTBRIDGE_ROLE_NAME}")
            time.sleep(5)
        else:
            raise

    # Inline policy: allow running our specific task + passing both roles
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "RunECSTask",
                "Effect": "Allow",
                "Action": "ecs:RunTask",
                "Resource": f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:task-definition/{TASK_DEF_FAMILY}:*",
            },
            {
                "Sid": "PassRolesToECS",
                "Effect": "Allow",
                "Action": "iam:PassRole",
                "Resource": [task_role_arn, exec_role_arn],
            },
        ],
    }
    iam.put_role_policy(
        RoleName=EVENTBRIDGE_ROLE_NAME,
        PolicyName="mini-loading-pipeline-eb-ecs-policy",
        PolicyDocument=json.dumps(policy),
    )
    print(f"  Inline policy attached to {EVENTBRIDGE_ROLE_NAME}")
    return eb_role_arn


def create_eventbridge_schedule(
    task_def_arn: str,
    eb_role_arn: str,
    task_role_arn: str,
    exec_role_arn: str,
    subnet_id: str,
    sg_id: str,
) -> None:
    print("\n[10] EventBridge Scheduler → ECS (direct, no Lambda)")
    scheduler = c("scheduler")

    schedule_name = EB_RULE_NAME
    cluster_arn   = f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:cluster/{settings.ECS_CLUSTER_NAME}"

    ecs_target = {
        "TaskDefinitionArn": task_def_arn,
        "LaunchType": "FARGATE",
        "NetworkConfiguration": {
            "awsvpcConfiguration": {
                "Subnets": [subnet_id],
                "SecurityGroups": [sg_id],
                "AssignPublicIp": "ENABLED",
            }
        },
        "TaskCount": 1,
    }

    try:
        scheduler.create_schedule(
            Name=schedule_name,
            ScheduleExpression="rate(15 minutes)",
            ScheduleExpressionTimezone="UTC",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn":     cluster_arn,
                "RoleArn": eb_role_arn,
                "EcsParameters": ecs_target,
            },
            State="ENABLED",
            Description="Triggers USGS ingest ECS task every 15 min (no Lambda)",
        )
        print(f"  Schedule created: {schedule_name} (rate: 15 minutes)")
    except scheduler.exceptions.ConflictException:
        # Schedule already exists — update it
        scheduler.update_schedule(
            Name=schedule_name,
            ScheduleExpression="rate(15 minutes)",
            ScheduleExpressionTimezone="UTC",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={
                "Arn": cluster_arn,
                "RoleArn": eb_role_arn,
                "EcsParameters": ecs_target,
            },
            State="ENABLED",
        )
        print(f"  Schedule updated: {schedule_name}")

def main() -> None:
    print("=" * 60)
    print("Mini Loading Pipeline — Ingest Deployment")
    print("Architecture: EventBridge Scheduler → ECS Fargate (no Lambda)")
    print("=" * 60)

    ecr_uri = create_ecr_repo()
    image_uri = build_and_push_image()
    create_ecs_cluster()
    create_log_group()
    task_role_arn, exec_role_arn = create_iam_roles()
    task_def_arn = register_task_definition(image_uri, task_role_arn, exec_role_arn)
    subnet_id, sg_id = get_default_vpc_config()
    eb_role_arn = create_eventbridge_role(task_role_arn, exec_role_arn)
    create_eventbridge_schedule(task_def_arn, eb_role_arn, task_role_arn, exec_role_arn, subnet_id, sg_id)

    print("\n" + "=" * 60)
    print("Deployment complete.")
    print(f"  ECS cluster    : {settings.ECS_CLUSTER_NAME}")
    print(f"  Task def       : {TASK_DEF_FAMILY}")
    print(f"  ECR image      : {image_uri}")
    print(f"  Schedule       : {EB_RULE_NAME} (rate: 15 minutes)")
    print(f"  EventBridge    : triggers ECS directly — no Lambda")
    print(f"  Subnet         : {subnet_id}")
    print(f"  Security group : {sg_id}")
    print()
    print("EventBridge Scheduler fires every 15 min → starts ECS task directly.")
    print("Check ECS → Clusters → Tasks for running ingest tasks.")
    print("Check CloudWatch → Log groups →", LOG_GROUP_ECS)
    print("=" * 60)


if __name__ == "__main__":
    main()
