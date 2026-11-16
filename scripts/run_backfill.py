from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.load_creds import load, load_config
load()
load_config()

import argparse
from datetime import date, timedelta

import boto3

from pipeline import settings
from pipeline.ingest.backfill import run_backfill


def _verify(start: date, end: date) -> None:
    days = (end - start).days + 1
    expected = [
        f"{settings.PREFIX_RAW_HISTORY}/{(start + timedelta(days=i)).isoformat()}.geojson"
        for i in range(days)
    ]

    s3 = boto3.Session(region_name=settings.AWS_REGION).client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=settings.BUCKET,
        Prefix=settings.PREFIX_RAW_HISTORY + "/",
    )
    found = {obj["Key"] for page in pages for obj in page.get("Contents", [])}

    print("\n=== F2 VERIFICATION ===")
    all_pass = True
    for key in expected:
        ok = key in found
        print(f"  [{'PASS' if ok else 'FAIL'}]  s3://{settings.BUCKET}/{key}")
        if not ok:
            all_pass = False

    total = sum(1 for k in expected if k in found)
    print(f"\n{'F2 PASSED' if all_pass else 'F2 FAILED'}: {total}/{days} files in S3.")
    if not all_pass:
        sys.exit(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Backfill USGS history to S3 (F2).")
    p.add_argument("--start", required=True, help="First date, YYYY-MM-DD (UTC)")
    p.add_argument("--end",   required=True, help="Last date inclusive, YYYY-MM-DD (UTC)")
    args = p.parse_args()

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)

    keys = run_backfill(start_date, end_date)

    print(f"\nFiles uploaded this run : {len(keys)}")
    for k in keys:
        print(f"  s3://{settings.BUCKET}/{k}")

    _verify(start_date, end_date)
