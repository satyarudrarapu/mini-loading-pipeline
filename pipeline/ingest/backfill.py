from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, timedelta

import boto3
import botocore.exceptions
import requests

from pipeline import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def _history_key(day: date) -> str:
    return f"{settings.PREFIX_RAW_HISTORY}/{day.isoformat()}.geojson"

def _make_s3_client() -> object:
    use_profile = (
        None
        if os.environ.get("AWS_ACCESS_KEY_ID")
        else (settings.AWS_PROFILE or None)
    )
    session = boto3.Session(
        profile_name=use_profile,
        region_name=settings.AWS_REGION,
    )
    return session.client("s3")


def _already_uploaded(s3_client, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=settings.BUCKET, Key=key)
        return True
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] == "404":
            return False
        raise

def fetch_history_day(day: date) -> bytes:
    start = day.isoformat()               
    end = (day + timedelta(days=1)).isoformat()  

    params = {
        "format": "geojson",
        "starttime": start,
        "endtime": end,
    }

    response = requests.get(
        settings.USGS_HISTORY_URL,
        params=params,
        timeout=settings.USGS_REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.content

def run_backfill(start: date, end: date) -> list[str]:
    if end < start:
        raise ValueError(f"end ({end}) must be >= start ({start})")

    s3 = _make_s3_client()
    uploaded: list[str] = []

    current = start
    while current <= end:
        key = _history_key(current)

        if _already_uploaded(s3, key):
            logger.info(json.dumps({
                "event": "file_skipped",
                "reason": "already_exists",
                "key": key,
            }))
            current += timedelta(days=1)
            continue

        body = fetch_history_day(current)

        s3.put_object(
            Bucket=settings.BUCKET,
            Key=key,
            Body=body,
            ContentType="application/geo+json",
        )

        log_record = {
            "event": "file_ingested",
            "table": "usgs.earthquakes",
            "key": key,
            "byte_count": len(body),
            "data_date": current.isoformat(),
        }
        logger.info(json.dumps(log_record))
        uploaded.append(key)

        current += timedelta(days=1)

    logger.info(
        json.dumps({
            "event": "backfill_complete",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "files_uploaded": len(uploaded),
        })
    )
    return uploaded

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill USGS earthquake history to S3 (F2).",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date inclusive, format YYYY-MM-DD (UTC).",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date inclusive, format YYYY-MM-DD (UTC).",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    keys = run_backfill(start_date, end_date)
    print(f"\nUploaded {len(keys)} file(s):")
    for k in keys:
        print(f"  s3://{settings.BUCKET}/{k}")
