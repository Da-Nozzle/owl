import pytest

from leads_ingest.sources.vmi.settings import VmiSettings, load_vmi_settings
from leads_testing.config import build_config


def test_vmi_settings_are_read_from_config() -> None:
    # Arrange
    config = build_config(
        {
            "vmi.register_url": "https://vmi.test/register",
            "vmi.activities_url": "https://vmi.test/activities",
            "vmi.daily_changes_schedule": "0 6 * * *",
            "vmi.first_daily_changes": "2026-08-01",
            "vmi.monthly_register_schedule": "0 6 21 * *",
            "vmi.changelog_page_size": 5_000,
            "vmi.register_export_attempts": 3,
            "vmi.register_export_wait_seconds": 61.5,
        }
    )

    # Act
    settings = load_vmi_settings(config)

    # Assert
    assert settings == VmiSettings(
        register_url="https://vmi.test/register",
        activities_url="https://vmi.test/activities",
        daily_changes_schedule="0 6 * * *",
        first_daily_changes="2026-08-01",
        monthly_register_schedule="0 6 21 * *",
        changelog_page_size=5_000,
        register_export_attempts=3,
        register_export_wait_seconds=61.5,
    )


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("changelog_page_size", 0),
        ("register_export_attempts", 0),
        ("register_export_wait_seconds", -1),
    ],
)
def test_a_count_or_wait_out_of_range_is_rejected(key: str, value: float) -> None:
    # Arrange
    config = build_config({f"vmi.{key}": value})

    # Act / Assert
    with pytest.raises(ValueError, match=rf"vmi\.{key}"):
        load_vmi_settings(config)
