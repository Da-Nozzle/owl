import pytest

from owl_ingest.sources.vmi_taxes_paid.settings import (
    VmiTaxesPaidSettings,
    load_vmi_taxes_paid_settings,
)
from owl_testing.config import build_config


def test_vmi_taxes_paid_settings_are_read_from_config() -> None:
    # Arrange
    config = build_config(
        {
            "vmi_taxes_paid.model_url": "https://vmi.test/Moketojas",
            "vmi_taxes_paid.snapshot_schedule": "0 6 21 * *",
            "vmi_taxes_paid.first_snapshot": "2025-06",
            "vmi_taxes_paid.changelog_page_size": 20_000,
        }
    )

    # Act
    settings = load_vmi_taxes_paid_settings(config)

    # Assert
    assert settings == VmiTaxesPaidSettings(
        model_url="https://vmi.test/Moketojas",
        snapshot_schedule="0 6 21 * *",
        first_snapshot="2025-06",
        changelog_page_size=20_000,
    )


def test_an_empty_changelog_page_is_rejected() -> None:
    # Arrange
    config = build_config({"vmi_taxes_paid.changelog_page_size": 0})

    # Act / Assert
    with pytest.raises(ValueError, match=r"vmi_taxes_paid\.changelog_page_size"):
        load_vmi_taxes_paid_settings(config)
