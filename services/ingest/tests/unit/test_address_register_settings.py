from leads_ingest.sources.address_register.settings import (
    AddressRegisterSettings,
    load_address_register_settings,
)
from leads_testing.config import build_config


def test_address_register_settings_are_read_from_config() -> None:
    # Arrange
    config = build_config(
        {
            "address_register.download_url_template": "https://ar.test/{dataset}.csv",
            "address_register.snapshot_schedule": "0 6 21 * *",
        }
    )

    # Act
    settings = load_address_register_settings(config)

    # Assert
    assert settings == AddressRegisterSettings(
        download_url_template="https://ar.test/{dataset}.csv", snapshot_schedule="0 6 21 * *"
    )
