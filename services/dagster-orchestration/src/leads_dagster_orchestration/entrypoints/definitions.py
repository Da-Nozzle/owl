from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject
from dlt.common.storages.configuration import FilesystemConfiguration

from leads_config.loading import load_configuration
from leads_dagster_orchestration.assets.sodra import build_sodra_archive_asset
from leads_dagster_orchestration.assets.transform import (
    build_transform_assets,
    ensure_metadata_database,
)
from leads_dagster_orchestration.partitions import replay_month_partitions, run_month_partitions
from leads_dagster_orchestration.tool_environments import (
    dagster_dbt_environment,
    dbt_main_state_dir,
)
from leads_ingest.adapters.archive.fsspec_archive import FsspecRawArchive, open_fsspec_archive
from leads_ingest.adapters.http.downloader import HttpDownloader
from leads_ingest.adapters.s3.configuration import filesystem_configuration
from leads_ingest.months import Month
from leads_ingest.settings import (
    PipelineMode,
    PipelineSettings,
    load_pipeline_settings,
    raw_archive_path,
)
from leads_ingest.sources.sodra.files import SOURCE_NAME, archive_yearly_files
from leads_ingest.sources.sodra.settings import load_sodra_settings
from leads_lake.settings import LakeStorage, load_lake_settings


@dg.definitions
def pipeline_definitions() -> dg.Definitions:
    config = load_configuration()
    lake_settings = load_lake_settings(config)
    pipeline_settings = load_pipeline_settings(config)
    archive = build_raw_archive(pipeline_settings, lake_settings.storage)
    downloader = (
        HttpDownloader(pipeline_settings.user_agent)
        if pipeline_settings.mode is PipelineMode.LIVE
        else None
    )
    sodra_settings = load_sodra_settings(config)
    partitions = (
        run_month_partitions(pipeline_settings)
        if pipeline_settings.mode is PipelineMode.LIVE
        else replay_month_partitions(archive.months(SOURCE_NAME), pipeline_settings)
    )

    def archive_sodra_files(run_month: Month) -> list[str]:
        return archive_yearly_files(run_month, archive, sodra_settings, downloader)

    transform_project = DbtProject(project_dir=config.get_str("transform.project_dir"))
    transform_project.prepare_if_dev()
    ensure_metadata_database(Path(dagster_dbt_environment(config)["LEADS_DBT_DUCKDB_PATH"]))
    return dg.Definitions(
        assets=[
            # TODO replay: build_sodra_archive_spec(partitions) instead.
            build_sodra_archive_asset(partitions, archive_sodra_files),
            *build_transform_assets(
                transform_project, partitions, Path(dbt_main_state_dir(config))
            ),
        ],
        resources={"dbt": DbtCliResource(project_dir=transform_project)},
    )


def build_raw_archive(settings: PipelineSettings, storage: LakeStorage) -> FsspecRawArchive:
    path = raw_archive_path(settings, storage)
    if settings.mode is PipelineMode.LIVE:
        return open_fsspec_archive(filesystem_configuration(path, storage))
    return open_fsspec_archive(FilesystemConfiguration(bucket_url=path))
