"""Load monthly ANAC flight files (VRA) into the raw layer of the S3 data lake."""

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from collections import Counter
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

# Calendar months checked on a default run, ending in the current one: the 12 latest
# published months, where ANAC's corrections usually land, plus the month still to come.
LOOKBACK_MONTHS = 13


def month_url(year: int, month: int) -> str:
    """Return the ANAC URL of the VRA file for one month."""
    folder = MONTH_FOLDERS[month - 1]
    # The folder pads the month ("09 - Setembro") but the file name does not (VRA_20269.csv).
    file_name = f"VRA_{year}{month}.csv"
    return quote(f"{BASE_URL}/{year}/{folder}/{file_name}", safe=":/")


def download(url: str) -> bytes | None:
    """Return the raw bytes of one month's file, or None if ANAC has not published it yet."""
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


def read_state(s3, bucket: str, key: str = STATE_KEY) -> dict:
    """Return the ingest state stored in the bucket, or an empty one on the first run."""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        return {}
    with response["Body"] as body:
        return json.loads(body.read())


def write_state(s3, bucket: str, state: dict, key: str = STATE_KEY) -> None:
    """Save the ingest state to the bucket, replacing the previous one."""
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(state, indent=2, sort_keys=True),
        ContentType="application/json",
    )


def raw_key(year: int, month: int, ingestion_date: str) -> str:
    """Return the S3 key where one version of a month is stored in the raw layer."""
    return f"raw/vra/month={year}-{month:02d}/ingestion_date={ingestion_date}/VRA_{year}{month}.csv"


def ingest_month(
    s3, bucket: str, state: dict, year: int, month: int, today: str, url_for=month_url, key_for=raw_key
) -> str:
    """Store one month in the raw layer if it is new or changed, and update the state in place.

    url_for and key_for default to the VRA files; other monthly ANAC sources pass their own.
    Returns what happened: "not published", "unchanged", "new" or "changed".
    """
    content = download(url_for(year, month))
    if content is None:
        return "not published"

    month_id = f"{year}-{month:02d}"
    digest = data_hash(content)
    previous = state.get(month_id)

    if previous is not None and previous["sha256"] == digest:
        previous["last_checked"] = today
        return "unchanged"

    # The file goes to S3 exactly as downloaded; cleaning happens later, in dbt.
    s3.put_object(Bucket=bucket, Key=key_for(year, month, today), Body=content, ContentType="text/csv")
    state[month_id] = {
        "sha256": digest,
        "ingestion_date": today,
        "rows": count_rows(content),
        "bytes": len(content),
        "last_checked": today,
    }
    return "new" if previous is None else "changed"


def months_between(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    """Return every (year, month) from start to end, both included."""
    year, month = start
    months = []
    while (year, month) <= end:
        months.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def lookback_start(today: date) -> tuple[int, int]:
    """Return the first month of the lookback window that ends in today's month."""
    # Counting months from year 0 turns "go back N months" into a subtraction.
    index = today.year * 12 + (today.month - 1) - (LOOKBACK_MONTHS - 1)
    return index // 12, index % 12 + 1


def parse_month(text: str) -> tuple[int, int]:
    """Turn "2026-09" into (2026, 9) for the --month option."""
    try:
        year, month = (int(part) for part in text.split("-"))
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {text!r}") from None
    if not 1 <= month <= 12:
        raise argparse.ArgumentTypeError(f"month must be between 01 and 12, got {text!r}")
    return year, month


def stored_line(results: Counter) -> str:
    """Return the line every ingest prints last, with how many files it stored."""
    # The Airflow DAG reads this line to decide whether dbt has anything to rebuild.
    return f"stored={results['new'] + results['changed']}"


def months_to_check(description: str) -> list[tuple[int, int]]:
    """Read --month or --since from the command line and return the months to check."""
    parser = argparse.ArgumentParser(
        description=description,
        epilog=f"Without options, checks the last {LOOKBACK_MONTHS} calendar months.",
    )
    which = parser.add_mutually_exclusive_group()
    which.add_argument("--month", type=parse_month, help="load only this month, as YYYY-MM")
    which.add_argument(
        "--since", type=parse_month, help="load every month from YYYY-MM until today (backfill or full check)"
    )
    args = parser.parse_args()

    today = date.today()
    current = (today.year, today.month)
    if args.month:
        return [args.month]
    if args.since:
        return months_between(args.since, current)
    return months_between(lookback_start(today), current)


def run(months: list[tuple[int, int]], state_key: str, url_for=month_url, key_for=raw_key) -> int:
    """Check each month against the stored state and print a summary; returns the exit code."""
    # load_dotenv must run before boto3.client so that AWS_PROFILE from .env is used.
    load_dotenv()
    bucket = os.environ["LAKE_BUCKET"]
    s3 = boto3.client("s3")
    today = date.today().isoformat()

    results = Counter()
    try:
        state = read_state(s3, bucket, state_key)
        for year, month in months:
            result = ingest_month(s3, bucket, state, year, month, today, url_for, key_for)
            results[result] += 1
            print(f"{year}-{month:02d}: {result}")
            if result in ("new", "changed"):
                # Saving right after each stored file means a failure later in the run cannot
                # leave a stored month out of the state, which would store it again tomorrow
                # under a new ingestion_date.
                write_state(s3, bucket, state, state_key)
        # One last save records last_checked for the unchanged months.
        write_state(s3, bucket, state, state_key)
    except (BotoCoreError, ClientError) as error:
        print(f"AWS error: {error}", file=sys.stderr)
        print("If the SSO session expired, run: aws sso login --profile pessoal", file=sys.stderr)
        return 1

    print("summary:", ", ".join(f"{count} {result}" for result, count in results.most_common()) or "nothing to do")
    print(stored_line(results))
    return 0


def main() -> int:
    return run(months_to_check(__doc__), STATE_KEY)


if __name__ == "__main__":
    sys.exit(main())
