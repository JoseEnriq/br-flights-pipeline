import pytest

from create_raw_tables import ATHENA_DIR, DDL_FILES, render_ddl
from repair_partitions import RAW_TABLES


@pytest.mark.parametrize("name", DDL_FILES)
def test_render_ddl_fills_the_bucket_and_drops_the_semicolon(name):
    ddl = render_ddl(ATHENA_DIR / name, "some-other-bucket")
    assert ddl.startswith("CREATE EXTERNAL TABLE IF NOT EXISTS br_flights.")
    assert "LOCATION 's3://some-other-bucket/raw/" in ddl
    assert "${" not in ddl
    assert not ddl.endswith(";")


def test_every_raw_table_has_a_ddl_file():
    created = [render_ddl(ATHENA_DIR / name, "b").split()[6] for name in DDL_FILES]
    assert created == RAW_TABLES


def test_no_ddl_file_names_a_bucket():
    # A bucket name written in a file would only work in one account
    for path in ATHENA_DIR.glob("*.sql"):
        assert "s3://${lake_bucket}/" in path.read_text(encoding="utf-8"), path.name
