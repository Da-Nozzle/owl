import pytest

from owl_config.loading import load_configuration
from owl_dagster_orchestration.archive.index import load_archive_index_settings
from owl_dagster_orchestration.dbt.environment import dagster_dbt_environment, dbt_environment
from owl_dagster_orchestration.dev_tools.dagster_dev import dagster_storage_environment
from owl_dagster_orchestration.dev_tools.dbt_cli import dbt_dev_environment
from owl_dagster_orchestration.dev_tools.mcp_servers import load_mcp_settings
from owl_dagster_orchestration.lineage.settings import load_lineage_settings
from owl_http.settings import load_http_client_settings
from owl_ingest.settings import PipelineMode, load_pipeline_settings
from owl_ingest.sources.address_register.settings import load_address_register_settings
from owl_ingest.sources.google_places.settings import load_google_places_settings
from owl_ingest.sources.jar.settings import load_jar_settings
from owl_ingest.sources.sodra.settings import load_sodra_settings
from owl_ingest.sources.vmi.settings import load_vmi_settings
from owl_lake.settings import load_lake_settings

HTTP_TABLES = [
    "sodra.http",
    "jar.http",
    "address_register.http",
    "vmi.daily_changes_http",
    "vmi.monthly_register_http",
    "google_places.http",
]


def test_the_local_config_loads_into_every_packages_settings(pytestconfig: pytest.Config) -> None:
    # Arrange
    config = load_configuration(pytestconfig.rootpath / "config" / "local")

    # Act / Assert
    load_pipeline_settings(config)
    load_lake_settings(config)
    load_lake_settings(config, replay=True)
    load_sodra_settings(config)
    load_jar_settings(config)
    load_address_register_settings(config)
    load_vmi_settings(config)
    load_google_places_settings(config)
    for key in HTTP_TABLES:
        load_http_client_settings(config, key)
    dagster_storage_environment(config)
    config.get_str("dagster.workspace")
    for mode in PipelineMode:
        dbt_environment(config, mode)
        dagster_dbt_environment(config, mode)
        dbt_dev_environment(config, mode)
    load_mcp_settings(config)
    load_lineage_settings(config)
    load_archive_index_settings(config)


def test_googles_client_retries_for_longer_than_a_minute_of_quota(
    pytestconfig: pytest.Config,
) -> None:
    # Arrange: the Places API's quota is per minute, and a sweep sends past it on purpose.
    config = load_configuration(pytestconfig.rootpath / "config" / "local")
    retry = load_http_client_settings(config, "google_places.http").retry

    # Act
    waited = sum(retry.wait_seconds(attempt) for attempt in range(1, retry.attempts))

    # Assert
    assert waited > 60
