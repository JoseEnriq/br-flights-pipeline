"""Create the raw tables in the Glue catalog and register the partitions already in S3.

The table definitions live in athena/raw_*.sql with the bucket written as
${lake_bucket}; this fills it from LAKE_BUCKET, so another account only changes
the .env. Run once when setting the project up, after Terraform has created the
bucket and the Glue databases.
"""

import os
import sys
from pathlib import Path
from string import Template

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

from repair_partitions import RAW_TABLES, run_query

ATHENA_DIR = Path(__file__).resolve().parent.parent / "athena"

DDL_FILES = ["raw_vra.sql", "raw_anac_stats.sql", "raw_anac_punctuality.sql"]


def render_ddl(path: Path, bucket: str) -> str:
    """Return the statement in one DDL file with the bucket filled in."""
    # The closing semicolon is for the console; the API takes one statement without it
    return Template(path.read_text(encoding="utf-8")).substitute(lake_bucket=bucket).strip().rstrip(";")


def main() -> int:
    # load_dotenv must run before boto3.client so that AWS_PROFILE from .env is used.
    load_dotenv()
    bucket = os.environ["LAKE_BUCKET"]
    athena = boto3.client("athena")
    try:
        for name in DDL_FILES:
            run_query(athena, render_ddl(ATHENA_DIR / name, bucket))
            print(f"{name}: table created, or already there")
        for table in RAW_TABLES:
            run_query(athena, f"MSCK REPAIR TABLE {table}")
            print(f"{table}: partitions registered")
    except (BotoCoreError, ClientError) as error:
        print(f"AWS error: {error}", file=sys.stderr)
        print("If the AWS SSO session expired, run aws sso login for the profile in AWS_PROFILE", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
