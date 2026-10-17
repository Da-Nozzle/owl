from leads_ingest.sources.jar.settings import JarSettings, load_jar_settings
from leads_testing.config import build_config


def test_jar_settings_are_read_from_config() -> None:
    # Arrange
    config = build_config(
        {
            "jar.download_url_template": "https://jar.test/{dataset}.csv",
            "jar.snapshot_schedule": "0 6 21 * *",
        }
    )

    # Act
    settings = load_jar_settings(config)

    # Assert
    assert settings == JarSettings(
        download_url_template="https://jar.test/{dataset}.csv", snapshot_schedule="0 6 21 * *"
    )
