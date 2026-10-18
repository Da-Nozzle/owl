import duckdb

from leads_ingest.adapters.ducklake.reader import reader_statements
from leads_lake.settings import load_lake_settings
from leads_testing.config import build_config


def test_the_reader_attaches_the_lake_read_only_and_capped() -> None:
    # Arrange
    settings = load_lake_settings(build_config({"lake.s3.secret": "a'b"}))

    # Act
    statements = reader_statements(settings, "1GB")

    # Assert
    parsed = [duckdb.extract_statements(statement)[-1].type.name for statement in statements]
    assert parsed[0] == "SET"
    assert parsed[-2:] == ["CREATE", "ATTACH"]
    assert statements[0] == "SET memory_limit = '1GB'"
    assert statements[-1].endswith(
        " AS lake (DATA_PATH 's3://leads-lake/', METADATA_SCHEMA 'ducklake', READ_ONLY)"
    )
