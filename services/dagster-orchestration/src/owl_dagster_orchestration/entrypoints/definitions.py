from pathlib import Path

import dagster as dg

from owl_config.configuration import Configuration
from owl_config.loading import load_configuration
from owl_dagster_orchestration.archive.assets import (
    build_archive_spec,
    build_fetch_asset,
    build_planned_fetch_asset,
)
from owl_dagster_orchestration.archive.index import (
    build_archive_index,
    load_archive_index_settings,
)
from owl_dagster_orchestration.asset_keys import plan_key
from owl_dagster_orchestration.automation import (
    plan_automation_sensor,
    transform_automation_sensor,
)
from owl_dagster_orchestration.dbt.assets import build_transform_assets
from owl_dagster_orchestration.dbt.environment import dbt_main_state_dir
from owl_dagster_orchestration.dbt.project import load_transform_project
from owl_dagster_orchestration.dbt.resource import dbt_core_resource
from owl_dagster_orchestration.executors import sequential_in_process_executor
from owl_dagster_orchestration.jobs import fetch_job, fetch_schedule, rebuild_job
from owl_dagster_orchestration.lake_labels import lake_labels
from owl_dagster_orchestration.lineage.sensor import build_lineage_sensor
from owl_dagster_orchestration.lineage.settings import load_lineage_settings
from owl_dagster_orchestration.snapshots import ArchivedSource, snapshot_partitions
from owl_dagster_orchestration.sources.adapters import (
    LiveAdapters,
    build_live_adapters,
    build_raw_archive,
)
from owl_dagster_orchestration.sources.address_register import address_register_source
from owl_dagster_orchestration.sources.google_places import google_places_source
from owl_dagster_orchestration.sources.jar import jar_source
from owl_dagster_orchestration.sources.sodra import sodra_source
from owl_dagster_orchestration.sources.vmi import vmi_sources
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.settings import PipelineMode, load_pipeline_settings
from owl_lake.settings import load_lake_settings


def pipeline_definitions(mode: PipelineMode, config: Configuration | None = None) -> dg.Definitions:
    """One lake's code location. Only live fetches; replay builds from the committed sample."""
    config = config or load_configuration()
    pipeline = load_pipeline_settings(config)
    lake = load_lake_settings(config, replay=mode is PipelineMode.REPLAY)
    archive = build_raw_archive(mode, pipeline, lake.storage)
    labels = lake_labels(mode, lake)
    live = (
        build_live_adapters(config, pipeline.user_agent, lake)
        if mode is PipelineMode.LIVE
        else None
    )
    sources = archived_sources(config, pipeline.timezone, archive, live)

    project = load_transform_project(config, mode)
    partitions = {source.name: snapshot_partitions(source.name, mode) for source in sources}
    transform_assets = build_transform_assets(
        project,
        partitions,
        Path(dbt_main_state_dir(config, mode)),
        labels,
        mode,
        groups={source.name: source.group_path for source in sources},
    )
    fetched = [source for source in sources if source.fetches]
    fetch_jobs = {source.name: fetch_job(source) for source in fetched}
    planned = [source for source in fetched if source.plan is not None]
    return dg.Definitions(
        assets=[
            *(build_archive_spec(source, mode, partitions[source.name]) for source in sources),
            *(build_fetch_asset(source, archive, labels) for source in fetched if not source.plan),
            *(build_planned_fetch_asset(source, archive, labels) for source in planned),
            *transform_assets,
        ],
        jobs=[*fetch_jobs.values(), *(rebuild_job(source, mode) for source in sources)],
        schedules=[fetch_schedule(source, fetch_jobs[source.name]) for source in fetched],
        sensors=[
            build_archive_index(
                sources, archive, mode, partitions, labels, load_archive_index_settings(config)
            ),
            transform_automation_sensor(transform_assets),
            *(
                plan_automation_sensor(source, plan_key(source.name, PipelineMode.LIVE))
                for source in planned
            ),
            build_lineage_sensor(
                load_lineage_settings(config), pipeline.user_agent, pipeline.timezone
            ),
        ],
        resources={"dbt": dbt_core_resource(project)},
        executor=sequential_in_process_executor,
    )


def archived_sources(
    config: Configuration, timezone: str, archive: RawArchive, live: LiveAdapters | None
) -> list[ArchivedSource]:
    """Each with its dbt models in models/raw/<name>/ and models/staging/<name>/."""
    return [
        sodra_source(config, timezone, archive, live),
        jar_source(config, timezone, archive, live),
        address_register_source(config, timezone, archive, live),
        *vmi_sources(config, timezone, archive, live),
        google_places_source(config, timezone, archive, live),
    ]
