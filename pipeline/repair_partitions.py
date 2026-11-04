"""Register the new partitions of the raw tables in the Glue catalog.

Every new version an ingest stores lands in a new S3 folder, and Athena only
reads a folder once it is registered as a partition of its table.
"""

import sys
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

RAW_TABLES = [
    "br_flights.raw_vra",
    "br_flights.raw_anac_stats",
    "br_flights.raw_anac_punctuality",
]

WORKGROUP = "br-flights"


def run_query(athena, sql: str, poll_seconds: float = 1.0) -> None:
    """Run one statement in the project workgroup and wait for it to finish."""
    query_id = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] == "SUCCEEDED":
            return
        if status["State"] in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"{sql} {status['State']}: {status.get('StateChangeReason', 'no reason given')}")
        time.sleep(poll_seconds)


def main() -> int:
    # load_dotenv must run before boto3.client so that AWS_PROFILE from .env is used.
    load_dotenv()
    athena = boto3.client("athena")
    try:
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
