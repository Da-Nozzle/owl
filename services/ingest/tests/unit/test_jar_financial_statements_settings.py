import pytest

from owl_ingest.sources.jar_financial_statements.settings import (
    JarFinancialStatementsSettings,
    load_jar_financial_statements_settings,
)
from owl_testing.config import build_config


def test_jar_financial_statements_settings_are_read_from_config() -> None:
    # Arrange
    config = build_config(
        {
            "jar_financial_statements.download_url_template": "https://jar.test/{dataset}.csv",
            "jar_financial_statements.snapshot_schedule": "0 6 21 3,6,9,12 *",
            "jar_financial_statements.page_rows": 500_000,
            "jar_financial_statements.fetch_max_runtime_seconds": 14_400,
        }
    )

    # Act
    settings = load_jar_financial_statements_settings(config)

    # Assert
    assert settings == JarFinancialStatementsSettings(
        download_url_template="https://jar.test/{dataset}.csv",
        snapshot_schedule="0 6 21 3,6,9,12 *",
        page_rows=500_000,
        fetch_max_runtime_seconds=14_400,
    )


@pytest.mark.parametrize("key", ["page_rows", "fetch_max_runtime_seconds"])
def test_a_count_below_one_is_rejected(key: str) -> None:
    # Arrange
    config = build_config({f"jar_financial_statements.{key}": 0})

    # Act / Assert
    with pytest.raises(ValueError, match=rf"jar_financial_statements\.{key}"):
        load_jar_financial_statements_settings(config)
