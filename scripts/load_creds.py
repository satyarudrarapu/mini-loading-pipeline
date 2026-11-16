from __future__ import annotations

import json
import os
import subprocess
import sys

AWS_CLI = r"C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe"

LAMBDA_FUNCTION = "mini-loading-pipeline-ingest"
REGION = "ap-southeast-2"


def load(profile: str = "default") -> None:
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        return

    result = subprocess.run(
        [
            AWS_CLI,
            "configure", "export-credentials",
            "--format", "env-no-export",
            "--profile", profile,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print("", file=sys.stderr)
        print("ERROR: Could not load AWS credentials.", file=sys.stderr)
        print("Your session may have expired. Run:", file=sys.stderr)
        print("", file=sys.stderr)
        print(r'    & "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" login', file=sys.stderr)
        print("", file=sys.stderr)
        print(result.stderr.strip(), file=sys.stderr)
        sys.exit(1)

    for line in result.stdout.splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            os.environ[key.strip()] = value.strip()

def load_config() -> None:
    import importlib
    import boto3

    region = REGION
    injected = []

    def _set(key: str, value: str) -> None:
        if value and not os.environ.get(key):
            os.environ[key] = value
            injected.append(key)

    try:
        ecs = boto3.Session(region_name=region).client("ecs")

        ingest_family = "mini-loading-pipeline-ingest-task"
        td = ecs.describe_task_definition(taskDefinition=ingest_family)
        tdef = td["taskDefinition"]
        env_map = {
            e["name"]: e["value"]
            for c in tdef.get("containerDefinitions", [])
            for e in c.get("environment", [])
        }
        _set("PIPELINE_BUCKET", env_map.get("PIPELINE_BUCKET", ""))
        _set("ECS_CLUSTER_NAME", "mini-loading-pipeline-cluster")
        _set("ECS_INGEST_TASK_DEF", f"{ingest_family}:{tdef['revision']}")
        _set("ECS_INGEST_CONTAINER_NAME", tdef["containerDefinitions"][0]["name"])
        _set("ECS_TASK_ROLE_ARN", tdef.get("taskRoleArn", ""))
        _set("ECS_EXECUTION_ROLE_ARN", tdef.get("executionRoleArn", ""))

        scheduler = boto3.Session(region_name=region).client("scheduler")
        try:
            sched = scheduler.get_schedule(
                Name="mini-loading-pipeline-ingest-schedule"
            )
            net_cfg = (
                sched["Target"]
                .get("EcsParameters", {})
                .get("NetworkConfiguration", {})
                .get("awsvpcConfiguration", {})
            )
            subnets = net_cfg.get("Subnets", [])
            sgs = net_cfg.get("SecurityGroups", [])
            if subnets:
                _set("ECS_SUBNET_ID", subnets[0])
            if sgs:
                _set("ECS_SECURITY_GROUP_ID", sgs[0])
            _set("EVENTBRIDGE_ROLE_ARN", sched["Target"].get("RoleArn", ""))
        except Exception:
            pass   

        sort_family = "mini-loading-pipeline-sort-task"
        try:
            sd = ecs.describe_task_definition(taskDefinition=sort_family)
            sdef = sd["taskDefinition"]
            sort_env = {
                e["name"]: e["value"]
                for c in sdef.get("containerDefinitions", [])
                for e in c.get("environment", [])
            }
            _set("SQS_SORT_QUEUE_URL", sort_env.get("SQS_SORT_QUEUE_URL", ""))
            _set("SQS_LOAD_QUEUE_URL", sort_env.get("SQS_LOAD_QUEUE_URL", ""))
            _set("ECS_SORT_TASK_DEF",  f"{sort_family}:{sdef['revision']}")
            _set("ECS_SORT_CONTAINER_NAME",
                 sdef["containerDefinitions"][0]["name"])
        except Exception:
            pass 

        if os.environ.get("SQS_SORT_QUEUE_URL"):
            try:
                sqs = boto3.Session(region_name=region).client("sqs")
                qa = sqs.get_queue_attributes(
                    QueueUrl=os.environ["SQS_SORT_QUEUE_URL"],
                    AttributeNames=["RedrivePolicy"],
                )
                rdp = qa["Attributes"].get("RedrivePolicy", "{}")
                dlq_arn = json.loads(rdp).get("deadLetterTargetArn", "")
                if dlq_arn:
                    parts   = dlq_arn.split(":")
                    dlq_url = (
                        f"https://sqs.{region}.amazonaws.com"
                        f"/{parts[-2]}/{parts[-1]}"
                    )
                    _set("SQS_SORT_DLQ_URL", dlq_url)
            except Exception:
                pass

    except Exception as e:
        print(f"Warning: could not load config from ECS: {e}", file=sys.stderr)
        return

    if injected:
        import pipeline.settings as _settings
        importlib.reload(_settings)
