"""Load ANAC's monthly delay and cancellation percentages (Annex I) into the raw layer.

ANAC computes these from its own flight records, by airline, flight number and
route, so they are the reference the flight model is reconciled against. Same
monthly cycle and change detection as the VRA files.
"""

import sys
from urllib.parse import quote

from ingest import months_to_check, run

BASE_URL = "https://sistemas.anac.gov.br/dadosabertos/Voos e operações aéreas/Percentuais de atrasos e cancelamentos"

# Unlike the VRA folders, these are lowercase and "março" has no cedilla.
MONTH_FOLDERS = [
    "01 - janeiro",
    "02 - fevereiro",
    "03 - marco",
    "04 - abril",
    "05 - maio",
    "06 - junho",
    "07 - julho",
    "08 - agosto",
    "09 - setembro",
    "10 - outubro",
    "11 - novembro",
    "12 - dezembro",
]

STATE_KEY = "_state/anac_punctuality_ingest.json"


def month_url(year: int, month: int) -> str:
    """Return the ANAC URL of Annex I for one month."""
    # Annex I is the most detailed of the three and the only one in UTF-8, like the VRA;
    # Annexes II and III are sums of it, in Latin-1.
    return quote(f"{BASE_URL}/{year}/{MONTH_FOLDERS[month - 1]}/Anexo I.csv", safe=":/")


def raw_key(year: int, month: int, ingestion_date: str) -> str:
    """Return the S3 key where one version of a month's Annex I is stored."""
    return f"raw/anac_punctuality/month={year}-{month:02d}/ingestion_date={ingestion_date}/Anexo_I.csv"


if __name__ == "__main__":
    sys.exit(run(months_to_check(__doc__), STATE_KEY, month_url, raw_key))
