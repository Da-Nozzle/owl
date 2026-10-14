import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject
from dagster_dlt import DagsterDltResource

from leads_config.loading import load_configuration
from leads_dagster_orchestration.assets.lake_probe import build_lake_probe_assets
from leads_dagster_orchestration.assets.transform import build_transform_assets
from leads_ingest.adapters.ducklake.destination import (
    build_ducklake_destination,
    build_raw_pipeline,
)
from leads_lake.settings import load_lake_settings


@dg.definitions
def pipeline_definitions() -> dg.Definitions:
    config = load_configuration()
    lake_destination = build_ducklake_destination(load_lake_settings(config))
    transform_project = DbtProject(project_dir=config.get_str("transform.project_dir"))
    transform_project.prepare_if_dev()
    return dg.Definitions(
        assets=[
            build_lake_probe_assets(build_raw_pipeline("lake_probe", lake_destination)),
            build_transform_assets(transform_project),
        ],
        resources={
            "dlt": DagsterDltResource(),
            "dbt": DbtCliResource(project_dir=transform_project),
        },
    )
