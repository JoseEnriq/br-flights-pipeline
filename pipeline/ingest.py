"""Load monthly ANAC flight files (VRA) into the raw layer of the S3 data lake."""

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date
from urllib.parse import quote

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

# Kept readable on purpose; month_url() encodes the spaces and accents.
BASE_URL = "https://sistemas.anac.gov.br/dadosabertos/Voos e operações aéreas/Voo Regular Ativo (VRA)"

MONTH_FOLDERS = [
    "01 - Janeiro",
    "02 - Fevereiro",
    "03 - Março",
    "04 - Abril",
    "05 - Maio",
    "06 - Junho",
    "07 - Julho",
    "08 - Agosto",
    "09 - Setembro",
    "10 - Outubro",
    "11 - Novembro",
    "12 - Dezembro",
]

# The leading underscore keeps Glue and Athena from treating the state as data.
STATE_KEY = "_state/vra_ingest.json"


def month_url(year: int, month: int) -> str:
    """Return the ANAC URL of the VRA file for one month."""
    folder = MONTH_FOLDERS[month - 1]
    # The folder pads the month ("09 - Setembro") but the file name does not (VRA_20269.csv).
    file_name = f"VRA_{year}{month}.csv"
    return quote(f"{BASE_URL}/{year}/{folder}/{file_name}", safe=":/")


def download_month(year: int, month: int) -> bytes | None:
    """Return the raw bytes of one month's file, or None if ANAC has not published it yet."""
    url = month_url(year, month)
    try:
        with urllib.request.urlopen(url) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        # Future month folders already exist but are empty, so the file answers 404.
        if error.code == 404:
            return None
        raise


def data_hash(content: bytes) -> str:
    """Return the SHA-256 of a VRA file, ignoring its first line."""
    # The first line is "Atualizado em: <date>". ANAC rewrites it on every regeneration,
    # even when no flight changed, so it must not count as a change.
    _, _, data = content.partition(b"\n")
    return hashlib.sha256(data).hexdigest()


def count_rows(content: bytes) -> int:
    """Return the number of flights in a VRA file."""
    # Two lines come before the flights: "Atualizado em" and the header.
    return len(content.splitlines()) - 2


def read_state(s3, bucket: str) -> dict:
    """Return the ingest state stored in the bucket, or an empty one on the first run."""
    try:
        response = s3.get_object(Bucket=bucket, Key=STATE_KEY)
    except s3.exceptions.NoSuchKey:
        return {}
    with response["Body"] as body:
        return json.loads(body.read())


def write_state(s3, bucket: str, state: dict) -> None:
    """Save the ingest state to the bucket, replacing the previous one."""
    s3.put_object(
        Bucket=bucket,
        Key=STATE_KEY,
        Body=json.dumps(state, indent=2, sort_keys=True),
        ContentType="application/json",
    )


def raw_key(year: int, month: int, ingestion_date: str) -> str:
    """Return the S3 key where one version of a month is stored in the raw layer."""
    return f"raw/vra/month={year}-{month:02d}/ingestion_date={ingestion_date}/VRA_{year}{month}.csv"


def ingest_month(s3, bucket: str, state: dict, year: int, month: int, today: str) -> str:
    """Store one month in the raw layer if it is new or changed, and update the state in place.

    Returns what happened: "not published", "unchanged", "new" or "changed".
    """
    content = download_month(year, month)
    if content is None:
        return "not published"

    month_id = f"{year}-{month:02d}"
    digest = data_hash(content)
    previous = state.get(month_id)

    if previous is not None and previous["sha256"] == digest:
        previous["last_checked"] = today
        return "unchanged"

    # The file goes to S3 exactly as downloaded; cleaning happens later, in dbt.
    s3.put_object(Bucket=bucket, Key=raw_key(year, month, today), Body=content, ContentType="text/csv")
    state[month_id] = {
        "sha256": digest,
        "ingestion_date": today,
        "rows": count_rows(content),
        "bytes": len(content),
        "last_checked": today,
    }
    return "new" if previous is None else "changed"


def parse_month(text: str) -> tuple[int, int]:
    """Turn "2026-09" into (2026, 9) for the --month option."""
    try:
        year, month = (int(part) for part in text.split("-"))
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {text!r}") from None
    if not 1 <= month <= 12:
        raise argparse.ArgumentTypeError(f"month must be between 01 and 12, got {text!r}")
    return year, month


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", type=parse_month, required=True, help="month to load, as YYYY-MM")
    args = parser.parse_args()
    year, month = args.month

    # load_dotenv must run before boto3.client so that AWS_PROFILE from .env is used.
    load_dotenv()
    bucket = os.environ["LAKE_BUCKET"]
    s3 = boto3.client("s3")

    try:
        state = read_state(s3, bucket)
        result = ingest_month(s3, bucket, state, year, month, date.today().isoformat())
        # The state is saved only after the file is in S3: a crash in between just repeats the month.
        write_state(s3, bucket, state)
    except (BotoCoreError, ClientError) as error:
        print(f"AWS error: {error}", file=sys.stderr)
        print("If the SSO session expired, run: aws sso login --profile pessoal", file=sys.stderr)
        return 1

    print(f"{year}-{month:02d}: {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
