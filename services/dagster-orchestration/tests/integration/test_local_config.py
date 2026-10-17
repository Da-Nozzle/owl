import pytest

from leads_config.loading import load_configuration
from leads_dagster_orchestration.tool_environments import (
    dagster_dbt_environment,
    dagster_storage_environment,
    dbt_dev_environment,
    dbt_environment,
)
from leads_ingest.settings import PipelineMode, load_pipeline_settings
from leads_ingest.sources.address_register.settings import load_address_register_settings
from leads_ingest.sources.jar.settings import load_jar_settings
from leads_ingest.sources.sodra.settings import load_sodra_settings
from leads_lake.settings import load_lake_settings


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
    dagster_storage_environment(config)
    config.get_str("dagster.workspace")
    for mode in PipelineMode:
        dbt_environment(config, mode)
        dagster_dbt_environment(config, mode)
        dbt_dev_environment(config, mode)
