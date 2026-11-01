"""Load ANAC's air transport statistics file into the raw layer of the S3 data lake.

The file names every airline and airport by the same codes the VRA uses, so it
feeds the airline and airport dimensions. It is a single file covering 2000 to
today, regenerated daily and about 360 MB, so it is meant to be checked weekly.
"""

import io
import os
import sys
import urllib.request
from datetime import date
from urllib.parse import quote

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

from ingest import count_rows, data_hash, read_state, write_state

# Kept readable on purpose; quote() encodes the spaces and accents.
STATS_URL = quote(
    "https://sistemas.anac.gov.br/dadosabertos/Voos e operações aéreas/"
    "Dados Estatísticos do Transporte Aéreo/Dados_Estatisticos.csv",
    safe=":/",
)

STATE_KEY = "_state/anac_stats_ingest.json"

# Sends the file in 64 MB parts, in parallel, instead of one long request that
# has to start over if the connection drops.
UPLOAD_CONFIG = TransferConfig(multipart_threshold=64 * 1024**2, multipart_chunksize=64 * 1024**2)


def raw_key(ingestion_date: str) -> str:
    """Return the S3 key where one version of the statistics file is stored."""
    return f"raw/anac_stats/ingestion_date={ingestion_date}/Dados_Estatisticos.csv"


def ingest(s3, bucket: str, state: dict, content: bytes, today: str) -> str:
    """Store the file if it is new or changed, and update the state in place.

    Returns what happened: "unchanged", "new" or "changed".
    """
    digest = data_hash(content)
    if state.get("sha256") == digest:
        state["last_checked"] = today
        return "unchanged"

    result = "changed" if state else "new"
    s3.upload_fileobj(
        io.BytesIO(content),
        bucket,
        raw_key(today),
        ExtraArgs={"ContentType": "text/csv"},
        Config=UPLOAD_CONFIG,
    )
    state.clear()
    state.update(
        sha256=digest,
        ingestion_date=today,
        rows=count_rows(content),
        bytes=len(content),
        last_checked=today,
    )
    return result


def main() -> int:
    # load_dotenv must run before boto3.client so that AWS_PROFILE from .env is used.
    load_dotenv()
    bucket = os.environ["LAKE_BUCKET"]
    s3 = boto3.client("s3")
    today = date.today().isoformat()

    with urllib.request.urlopen(STATS_URL) as response:
        content = response.read()

    try:
        state = read_state(s3, bucket, STATE_KEY)
        result = ingest(s3, bucket, state, content, today)
        write_state(s3, bucket, state, STATE_KEY)
    except (BotoCoreError, ClientError) as error:
        print(f"AWS error: {error}", file=sys.stderr)
        print("If the SSO session expired, run: aws sso login --profile pessoal", file=sys.stderr)
        return 1

    print(f"statistics file: {result} ({state['rows']} rows, {state['bytes'] / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
