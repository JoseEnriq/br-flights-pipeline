import argparse
from collections import Counter
from datetime import date

import pytest

from ingest import (
    LOOKBACK_MONTHS,
    count_rows,
    data_hash,
    lookback_start,
    month_url,
    months_between,
    parse_month,
    raw_key,
    stored_line,
)

HEADER = b'"ICAO Empresa A\xc3\xa9rea";"N\xc3\xbamero Voo";"Situa\xc3\xa7\xc3\xa3o Voo"\r\n'
FLIGHTS = b'"GLO";"1234";"REALIZADO"\r\n"AZU";"4321";"CANCELADO"\r\n'


def vra_file(updated_on: str, flights: bytes = FLIGHTS) -> bytes:
    """Build a small file shaped like the ANAC ones: BOM, metadata line, header, flights."""
    return b"\xef\xbb\xbf" + f"Atualizado em: {updated_on}\r\n".encode() + HEADER + flights


def test_month_url_pads_the_folder_but_not_the_file_name():
    url = month_url(2026, 9)
    assert url.endswith("/2026/09%20-%20Setembro/VRA_20269.csv")


def test_month_url_encodes_accents_and_keeps_the_scheme():
    url = month_url(2024, 3)
    assert url.startswith("https://sistemas.anac.gov.br/")
    assert "/2024/03%20-%20Mar%C3%A7o/VRA_20243.csv" in url
    assert " " not in url


def test_data_hash_ignores_the_updated_on_line():
    assert data_hash(vra_file("2026-10-28")) == data_hash(vra_file("2026-10-29"))


def test_data_hash_changes_when_a_flight_changes():
    edited = FLIGHTS.replace(b"REALIZADO", b"CANCELADO")
    assert data_hash(vra_file("2026-10-28")) != data_hash(vra_file("2026-10-28", edited))


def test_data_hash_changes_when_the_header_changes():
    original = vra_file("2026-10-28")
    renamed = original.replace(b"Situa\xc3\xa7\xc3\xa3o Voo", b"Status")
    assert data_hash(original) != data_hash(renamed)


def test_count_rows_skips_the_metadata_line_and_the_header():
    assert count_rows(vra_file("2026-10-28")) == 2


def test_raw_key_pads_the_month_partition():
    key = raw_key(2026, 9, "2026-10-28")
    assert key == "raw/vra/month=2026-09/ingestion_date=2026-10-28/VRA_20269.csv"


def test_parse_month_reads_year_and_month():
    assert parse_month("2026-09") == (2026, 9)


@pytest.mark.parametrize("text", ["2026-13", "2026-00", "2026/09", "setembro", "2026-09-01"])
def test_parse_month_rejects_bad_input(text):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_month(text)


def test_months_between_crosses_the_year():
    assert months_between((2025, 11), (2026, 2)) == [(2025, 11), (2025, 12), (2026, 1), (2026, 2)]


def test_months_between_is_empty_when_start_is_after_end():
    assert months_between((2026, 11), (2026, 10)) == []


@pytest.mark.parametrize(
    ("today", "expected"),
    [
        (date(2026, 10, 28), (2025, 10)),
        (date(2026, 1, 15), (2025, 1)),
        (date(2026, 12, 31), (2025, 12)),
    ],
)
def test_lookback_start(today, expected):
    assert lookback_start(today) == expected


def test_lookback_window_has_the_configured_length():
    today = date(2026, 10, 28)
    assert len(months_between(lookback_start(today), (2026, 10))) == LOOKBACK_MONTHS


def test_stored_line_counts_only_new_and_changed_files():
    results = Counter({"unchanged": 11, "new": 2, "changed": 1, "not published": 3})
    assert stored_line(results) == "stored=3"


def test_stored_line_is_zero_when_nothing_was_stored():
    assert stored_line(Counter({"unchanged": 12})) == "stored=0"
