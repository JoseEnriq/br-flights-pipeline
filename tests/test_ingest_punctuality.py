from ingest_punctuality import month_url, raw_key


def test_month_url_uses_lowercase_folders_without_cedilla():
    url = month_url(2026, 3)
    assert url.endswith("/2026/03%20-%20marco/Anexo%20I.csv")


def test_month_url_encodes_the_path_and_keeps_the_scheme():
    url = month_url(2025, 10)
    assert url.startswith("https://sistemas.anac.gov.br/")
    assert "Percentuais%20de%20atrasos%20e%20cancelamentos/2025/10%20-%20outubro/" in url
    assert " " not in url


def test_raw_key_keeps_one_folder_per_month_and_version():
    key = raw_key(2026, 8, "2026-11-02")
    assert key == "raw/anac_punctuality/month=2026-08/ingestion_date=2026-11-02/Anexo_I.csv"
