import pytest

from leads_ingest.sources.sodra.settings import SodraSettings, load_sodra_settings
from leads_testing.config import build_config


def test_sodra_settings_are_read_from_config() -> None:
    # Arrange
    config = build_config(
        {
            "sodra.download_url_template": "https://sodra.test/{year}.zip",
            "sodra.history_months": 13,
            "sodra.snapshot_schedule": "0 6 21 * *",
        }
    )

    # Act
    settings = load_sodra_settings(config)

    # Assert
    assert settings == SodraSettings(
        "https://sodra.test/{year}.zip", history_months=13, snapshot_schedule="0 6 21 * *"
    )


def test_a_window_without_history_is_rejected() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=r"sodra\.history_months"):
        load_sodra_settings(build_config({"sodra.history_months": 0}))
