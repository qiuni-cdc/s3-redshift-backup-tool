"""
Load PROD raw tables from S3 prod_migration export.

Run on the server (prod worktree) after sourcing .env:
    python /tmp/load_raw_from_s3.py

What this does:
1. COPY from s3://redshift-dw-qa-uniuni-com/prod_migration/* into raw tables
2. DELETE rows outside Jan 1-24 window (1767225600 <= ts < 1769299200)
3. Print row counts after each step
"""

import psycopg2
import os
import sys

# ----- Connection -----
REDSHIFT_HOST = os.environ.get("DBT_REDSHIFT_HOST", "redshift-dw.uniuni.com")
REDSHIFT_PORT = int(os.environ.get("DBT_REDSHIFT_PORT", "5439"))
REDSHIFT_USER = os.environ.get("REDSHIFT_PRO_USER", "sett_ddl_owner")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PRO_PASSWORD")

if not REDSHIFT_PASSWORD:
    sys.exit("ERROR: REDSHIFT_PRO_PASSWORD not set. Run: source .env")

# S3 credentials (QA bucket)
AWS_KEY = os.environ.get("AWS_ACCESS_KEY_ID_QA", os.environ.get("AWS_ACCESS_KEY_ID"))
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY_QA", os.environ.get("AWS_SECRET_ACCESS_KEY"))
S3_BUCKET = "redshift-dw-qa-uniuni-com"

if not AWS_KEY or not AWS_SECRET:
    sys.exit("ERROR: AWS credentials not set. Run: source .env")

# Jan 1-24 2026 epoch boundaries
JAN_1  = 1767225600   # 2026-01-01 00:00:00 UTC
JAN_25 = 1769299200   # 2026-01-25 00:00:00 UTC  (exclusive)

TABLES = [
    {
        "raw_table":  "settlement_public.ecs_order_info_raw",
        "s3_prefix":  "prod_migration/ecs_raw/",
        "ts_col":     "add_time",
    },
    {
        "raw_table":  "settlement_public.uni_tracking_info_raw",
        "s3_prefix":  "prod_migration/uti_raw/",
        "ts_col":     "update_time",
    },
    {
        "raw_table":  "settlement_public.uni_tracking_spath_raw",
        "s3_prefix":  "prod_migration/uts_raw/",
        "ts_col":     "pathTime",
    },
]


def run():
    print(f"Connecting to {REDSHIFT_HOST}:{REDSHIFT_PORT} as {REDSHIFT_USER}...")
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname="dw",
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        connect_timeout=900,
    )
    conn.autocommit = True
    cur = conn.cursor()
    print("Connected.\n")

    # ------------------------------------------------------------------ #
    # Step 1: COPY from S3 into each raw table                           #
    # ------------------------------------------------------------------ #
    for t in TABLES:
        table   = t["raw_table"]
        prefix  = t["s3_prefix"]
        s3_path = f"s3://{S3_BUCKET}/{prefix}"

        print(f"[COPY] {table}  <--  {s3_path}")
        copy_sql = f"""
            COPY {table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{AWS_KEY}'
            SECRET_ACCESS_KEY '{AWS_SECRET}'
            FORMAT AS PARQUET;
        """
        try:
            cur.execute(copy_sql)
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"       -> {count:,} rows after COPY\n")
        except Exception as e:
            print(f"       ERROR: {e}")
            print("       Trying CSV format...")
            copy_csv_sql = f"""
                COPY {table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{AWS_KEY}'
                SECRET_ACCESS_KEY '{AWS_SECRET}'
                GZIP
                CSV
                IGNOREHEADER 1;
            """
            cur.execute(copy_csv_sql)
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"       -> {count:,} rows after CSV COPY\n")

    # ------------------------------------------------------------------ #
    # Step 2: DELETE rows outside Jan 1-24 window                        #
    # ------------------------------------------------------------------ #
    print("=" * 60)
    print(f"Trimming to Jan 1-24 window: [{JAN_1}, {JAN_25})")
    print("=" * 60)

    for t in TABLES:
        table  = t["raw_table"]
        ts_col = t["ts_col"]

        print(f"[DELETE] {table} WHERE {ts_col} < {JAN_1} OR {ts_col} >= {JAN_25}")
        del_sql = f"""
            DELETE FROM {table}
            WHERE {ts_col} < {JAN_1} OR {ts_col} >= {JAN_25};
        """
        cur.execute(del_sql)

        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"         -> {count:,} rows remaining\n")

    cur.close()
    conn.close()
    print("All done.")


if __name__ == "__main__":
    run()
