"""
seed_watermarks_to_now.py

Seeds all 3 order-tracking table watermarks to the current UTC time so the
pipeline starts in steady-state mode (only extracts new rows) instead of
crawling from ts=0 and permanently skipping rows when it hits the 100K LIMIT.

Usage:
    python seed_watermarks_to_now.py            # dry-run (shows what would be written)
    python seed_watermarks_to_now.py --apply    # writes watermarks to S3

Environment variables required:
    S3_ACCESS_KEY
    S3_SECRET_KEY
    S3_REGION   (optional, default: us-west-2)

What it writes
--------------
For each table a v2.0 watermark JSON is PUT to:
    s3://redshift-dw-qa-uniuni-com/watermarks/v2/<key>.json

The only field that drives extraction lower bound is:
    mysql_state.last_timestamp  (ISO 8601 UTC string)

All row-count fields are preserved at 0 so the pipeline treats this as a
fresh start with no prior history.
"""

import json
import os
import sys
from datetime import datetime, timezone

try:
    import boto3
except ImportError:
    print("ERROR: boto3 is not installed. Run: pip install boto3")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv optional


# ── Configuration ────────────────────────────────────────────────────────────

BUCKET = "redshift-dw-qa-uniuni-com"
WATERMARK_PREFIX = "watermarks/v2/"

# target_connection  →  matches pipeline YAML  target: "redshift_default_direct"
TARGET_CONNECTION = "redshift_default_direct"

# (display_name, mysql_table_name)
# Keys are built as: {schema}_{table}_{target_connection}
TABLES = [
    ("ecs_order_info",    "kuaisong.ecs_order_info"),
    ("uni_tracking_info", "kuaisong.uni_tracking_info"),
    ("uni_tracking_spath","kuaisong.uni_tracking_spath"),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_table_name(table_name: str, target_connection: str) -> str:
    """
    Replicates SimpleWatermarkManager._clean_table_name().
    Format: {table_lowercased_dots_to_underscores}_{target_connection}
    """
    base = table_name.replace(":", "_").replace(".", "_").lower()
    target = target_connection.replace(":", "_").replace(".", "_").lower()
    return f"{base}_{target}"


def build_watermark(table_name: str, now_iso: str) -> dict:
    """Build a v2.0 watermark with last_timestamp set to now."""
    return {
        "version": "2.0",
        "table_name": table_name,
        "cdc_strategy": "hybrid",
        "mysql_state": {
            "last_timestamp": now_iso,
            "last_id": None,
            "status": "success",
            "error": None,
            "total_rows": 0,
            "last_session_rows": 0,
            "s3_files_created": 0,
            "last_session_files": 0,
            "last_updated": now_iso
        },
        "redshift_state": {
            "total_rows": 0,
            "last_session_rows": 0,
            "last_updated": None,
            "status": "pending",
            "error": None,
            "last_loaded_files": []
        },
        "processed_files": [],
        "metadata": {
            "created_at": now_iso,
            "manual_override": True,
            "manual_set_reason": "seeded_to_now_by_seed_watermarks_to_now.py"
        }
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    apply = "--apply" in sys.argv

    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    region     = os.environ.get("S3_REGION", "us-west-2")

    if not access_key or not secret_key:
        print("ERROR: S3_ACCESS_KEY and S3_SECRET_KEY must be set in the environment or .env file")
        sys.exit(1)

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    print(f"\nSeeding watermarks to: {now_iso}")
    print(f"Bucket:               s3://{BUCKET}")
    print(f"Prefix:               {WATERMARK_PREFIX}")
    print(f"Mode:                 {'APPLY (writing to S3)' if apply else 'DRY-RUN (pass --apply to write)'}")
    print()

    if apply:
        s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    for display_name, table_name in TABLES:
        key_suffix = clean_table_name(table_name, TARGET_CONNECTION)
        s3_key     = f"{WATERMARK_PREFIX}{key_suffix}.json"
        watermark  = build_watermark(table_name, now_iso)
        body       = json.dumps(watermark, indent=2)

        print(f"  [{display_name}]")
        print(f"    S3 key:           {s3_key}")
        print(f"    last_timestamp:   {now_iso}")

        if apply:
            # Check what's currently there first
            try:
                existing = s3.get_object(Bucket=BUCKET, Key=s3_key)
                old = json.loads(existing["Body"].read())
                old_ts = old.get("mysql_state", {}).get("last_timestamp", "none")
                print(f"    Overwriting:      last_timestamp was {old_ts}")
            except s3.exceptions.NoSuchKey:
                print(f"    No existing watermark found — creating fresh")
            except Exception:
                print(f"    Could not read existing watermark (key may not exist)")

            s3.put_object(
                Bucket=BUCKET,
                Key=s3_key,
                Body=body.encode("utf-8"),
                ContentType="application/json"
            )
            print(f"    Status:           WRITTEN ✓")
        else:
            print(f"    Status:           DRY-RUN (not written)")
        print()

    if apply:
        print("All watermarks written. The next pipeline run will extract only rows")
        print(f"with timestamps > {now_iso}")
    else:
        print("Dry-run complete. Re-run with --apply to write to S3.")


if __name__ == "__main__":
    main()
