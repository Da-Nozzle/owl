import pytest

from owl_ingest.sources.vmi_tax_arrears.settings import (
    VmiTaxArrearsSettings,
    load_vmi_tax_arrears_settings,
)
from owl_testing.config import build_config


def test_vmi_tax_arrears_settings_are_read_from_config() -> None:
    # Arrange
    config = build_config(
        {
            "vmi_tax_arrears.model_url": "https://vmi.test/arrears",
            "vmi_tax_arrears.snapshot_schedule": "0 6 21 * *",
            "vmi_tax_arrears.first_snapshot": "2025-01",
            "vmi_tax_arrears.changelog_page_size": 5_000,
        }
    )

    # Act
    settings = load_vmi_tax_arrears_settings(config)

    # Assert
    assert settings == VmiTaxArrearsSettings(
        model_url="https://vmi.test/arrears",
        snapshot_schedule="0 6 21 * *",
        first_snapshot="2025-01",
        changelog_page_size=5_000,
    )


def test_a_page_size_below_one_is_rejected() -> None:
    # Arrange
    config = build_config({"vmi_tax_arrears.changelog_page_size": 0})

    # Act / Assert
    with pytest.raises(ValueError, match=r"vmi_tax_arrears\.changelog_page_size"):
        load_vmi_tax_arrears_settings(config)
