from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

import boto3
import requests

from pipeline import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",   
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _build_s3_key(now: datetime) -> str:
    date_part = now.strftime("%Y-%m-%d")
    time_part = now.strftime("%H%M%S")
    return f"{settings.PREFIX_RAW_ALL_DAY}/{date_part}/{time_part}.geojson"


def fetch_usgs_feed() -> bytes:
    response = requests.get(
        settings.USGS_ALL_DAY_URL,
        timeout=settings.USGS_REQUEST_TIMEOUT,
    )
    response.raise_for_status() 
    return response.content


def upload_to_s3(body: bytes, s3_key: str) -> None:
   
    inside_ecs = bool(os.environ.get("ECS_CONTAINER_METADATA_URI") or
                      os.environ.get("ECS_CONTAINER_METADATA_URI_V4"))
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
    s3 = session.client("s3")
    s3.put_object(
        Bucket=settings.BUCKET,
        Key=s3_key,
        Body=body,
        ContentType="application/geo+json",
    )

def run() -> None:
    now = _utc_now()
    s3_key = _build_s3_key(now)

    logger.info(json.dumps({
        "event": "ingest_started",
        "table": "usgs.earthquakes",
        "key": s3_key,
        "utc_time": now.isoformat(),
    }))

    body = fetch_usgs_feed()

    upload_to_s3(body, s3_key)

    logger.info(json.dumps({
        "event": "file_ingested",
        "table": "usgs.earthquakes",
        "key": s3_key,
        "byte_count": len(body),
        "utc_time": now.isoformat(),
    }))

if __name__ == "__main__":
    try:
        run()
        sys.exit(0)
    except Exception as exc:
        logger.error(json.dumps({
            "event": "ingest_failed",
            "error": str(exc),
            "error_type": type(exc).__name__,
        }))
        sys.exit(1)
