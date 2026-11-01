from ingest_stats import ingest, raw_key

HEADER = b'"EMPRESA_SIGLA";"EMPRESA_NOME";"ANO";"MES"\r\n'
ROWS = b'"AZU";"AZUL LINHAS A\xc3\x89REAS BRASILEIRAS S/A";"2026";"9"\r\n'


def stats_file(updated_on: str, rows: bytes = ROWS) -> bytes:
    """Build a small file shaped like ANAC's: BOM, metadata line, header, rows."""
    return b"\xef\xbb\xbf" + f"Atualizado em: {updated_on}\r\n".encode() + HEADER + rows


class FakeS3:
    """Records uploads instead of sending them, so ingest() runs without AWS."""

    def __init__(self):
        self.uploads = []

    def upload_fileobj(self, fileobj, bucket, key, ExtraArgs=None, Config=None):
        self.uploads.append((key, fileobj.read()))


def test_raw_key_has_one_folder_per_version():
    assert raw_key("2026-11-01") == "raw/anac_stats/ingestion_date=2026-11-01/Dados_Estatisticos.csv"


def test_first_run_stores_the_file_as_downloaded():
    s3, state = FakeS3(), {}
    content = stats_file("2026-11-01")

    assert ingest(s3, "lake", state, content, "2026-11-01") == "new"
    assert s3.uploads == [(raw_key("2026-11-01"), content)]
    assert state["rows"] == 1
    assert state["ingestion_date"] == "2026-11-01"


def test_regenerated_file_with_the_same_rows_is_not_stored_again():
    s3, state = FakeS3(), {}
    ingest(s3, "lake", state, stats_file("2026-11-01"), "2026-11-01")

    assert ingest(s3, "lake", state, stats_file("2026-11-08"), "2026-11-08") == "unchanged"
    assert len(s3.uploads) == 1
    assert state["ingestion_date"] == "2026-11-01"
    assert state["last_checked"] == "2026-11-08"


def test_changed_rows_are_stored_as_a_new_version():
    s3, state = FakeS3(), {}
    ingest(s3, "lake", state, stats_file("2026-11-01"), "2026-11-01")
    changed = stats_file("2026-11-08", ROWS + b'"GLO";"GOL LINHAS A\xc3\x89REAS S.A.";"2026";"9"\r\n')

    assert ingest(s3, "lake", state, changed, "2026-11-08") == "changed"
    assert [key for key, _ in s3.uploads] == [raw_key("2026-11-01"), raw_key("2026-11-08")]
    assert state["rows"] == 2
