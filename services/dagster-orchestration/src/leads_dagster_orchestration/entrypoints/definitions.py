from collections.abc import Sequence
from pathlib import Path

import dagster as dg
from dagster_dbt import DbtProject
from dlt.common.storages.configuration import FilesystemConfiguration

from leads_config.configuration import Configuration
from leads_config.loading import load_configuration
from leads_dagster_orchestration.archive_index import build_archive_index
from leads_dagster_orchestration.assets.archive import build_archive_spec, build_fetch_asset
from leads_dagster_orchestration.assets.transform import (
    build_transform_assets,
    ensure_metadata_database,
    prepare_on_initial_load,
    set_dbt_environment,
)
from leads_dagster_orchestration.automation import transform_automation_sensor
from leads_dagster_orchestration.dbt_resource import dbt_core_resource
from leads_dagster_orchestration.executors import sequential_in_process_executor
from leads_dagster_orchestration.jobs import fetch_job, fetch_schedule, rebuild_job
from leads_dagster_orchestration.lake_labels import lake_labels
from leads_dagster_orchestration.snapshots import (
    ArchivedSource,
    SnapshotCadence,
    snapshot_partitions,
)
from leads_dagster_orchestration.tool_environments import (
    dagster_dbt_environment,
    dbt_environment,
    dbt_main_state_dir,
)
from leads_ingest.adapters.archive.fsspec_archive import FsspecRawArchive, open_fsspec_archive
from leads_ingest.adapters.http.downloader import (
    API_TIMEOUT_SECONDS,
    HttpDownloader,
    ThrottledDownloader,
)
from leads_ingest.adapters.s3.configuration import filesystem_configuration
from leads_ingest.ports.downloader import Download, Downloader
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.settings import (
    PipelineMode,
    PipelineSettings,
    load_pipeline_settings,
    raw_archive_path,
)
from leads_ingest.sources import spinta
from leads_ingest.sources.jar import files as jar_files
from leads_ingest.sources.jar.settings import load_jar_settings
from leads_ingest.sources.sodra import files as sodra_files
from leads_ingest.sources.sodra.settings import load_sodra_settings
from leads_ingest.sources.vmi import daily_changes as vmi_daily_changes
from leads_ingest.sources.vmi import monthly_register as vmi_monthly_register
from leads_ingest.sources.vmi.settings import load_vmi_settings
from leads_lake.settings import LakeStorage, load_lake_settings

# The VMI register is two sources: its changes, daily, and the whole register, monthly, which the
# changes are applied to. Both are drawn in one box with the staging models that combine them,
# which are in models/staging/vmi/.
VMI_GROUP = "vmi"
VMI_DAILY_CHANGES_GROUP = f"{VMI_GROUP}/daily_changes"
VMI_MONTHLY_REGISTER_GROUP = f"{VMI_GROUP}/monthly_register"


def pipeline_definitions(mode: PipelineMode, config: Configuration | None = None) -> dg.Definitions:
    """One lake's code location (entrypoints/live.py, entrypoints/replay.py), built from its
    archived sources. Per source: its archive, partitioned by the snapshots the archive holds
    (the archive index adds them); its dbt models, raw partitioned the same way; and a job to
    rebuild from the archive. Live adds a fetch per source, with its job and schedule; replay's
    archive is the committed sample, which nothing fetches into. A new snapshot builds itself."""
    config = config or load_configuration()
    pipeline_settings = load_pipeline_settings(config)
    lake_settings = load_lake_settings(config, replay=mode is PipelineMode.REPLAY)
    archive = build_raw_archive(mode, pipeline_settings, lake_settings.storage)
    labels = lake_labels(mode, lake_settings)
    live = mode is PipelineMode.LIVE
    downloader = HttpDownloader(pipeline_settings.user_agent) if live else None
    # For get.data.gov.lt's API, read in many small requests: one that hangs is retried sooner,
    # and they are spaced, to stay under the portal's rate limit.
    api = (
        ThrottledDownloader(
            HttpDownloader(pipeline_settings.user_agent, API_TIMEOUT_SECONDS),
            spinta.REQUEST_INTERVAL_SECONDS,
        )
        if live
        else None
    )
    sources = archived_sources(config, pipeline_settings, archive, downloader, api)

    dbt_variables = dbt_environment(config, mode) | dagster_dbt_environment(config, mode)
    set_dbt_environment(dbt_variables)
    transform_project = DbtProject(
        project_dir=config.get_str("transform.project_dir"),
        target_path=dbt_variables["DBT_TARGET_PATH"],
    )
    prepare_on_initial_load(transform_project)
    ensure_metadata_database(Path(dbt_variables["LEADS_DBT_DUCKDB_PATH"]))

    partitions = {source.name: snapshot_partitions(source.name, mode) for source in sources}
    transform_assets = build_transform_assets(
        transform_project,
        partitions,
        Path(dbt_main_state_dir(config, mode)),
        labels,
        mode,
        groups={source.name: source.group_path for source in sources},
    )
    fetched = [source for source in sources if source.fetch is not None]
    fetch_jobs = {source.name: fetch_job(source) for source in fetched}
    return dg.Definitions(
        assets=[
            *(build_archive_spec(source, mode, partitions[source.name]) for source in sources),
            *(build_fetch_asset(source, archive, labels) for source in fetched),
            *transform_assets,
        ],
        jobs=[*fetch_jobs.values(), *(rebuild_job(source, mode) for source in sources)],
        schedules=[fetch_schedule(source, fetch_jobs[source.name]) for source in fetched],
        sensors=[
            build_archive_index(sources, archive, mode, partitions, labels),
            transform_automation_sensor(transform_assets),
        ],
        resources={"dbt": dbt_core_resource(transform_project)},
        executor=sequential_in_process_executor,
    )


def archived_sources(
    config: Configuration,
    settings: PipelineSettings,
    archive: RawArchive,
    downloader: Downloader | None,
    api: Downloader | None = None,
) -> Sequence[ArchivedSource]:
    """Every source that goes through the archive. A source is one entry here, its fetch (used
    only with a downloader, in the live lake: `downloader` for files, `api` for an API read in
    small requests) and its dbt models in models/raw/<name>/ and models/staging/<name>/."""
    sodra = load_sodra_settings(config)
    jar = load_jar_settings(config)
    vmi = load_vmi_settings(config)
    vmi_daily_cadence = SnapshotCadence(
        schedule=vmi.daily_changes_schedule,
        key_format=vmi_daily_changes.SNAPSHOT_KEY_FORMAT,
        timezone=settings.timezone,
    )

    def fetch_sodra(snapshot: str) -> dict[str, Download]:
        assert downloader is not None
        return sodra_files.archive_yearly_files(snapshot, archive, sodra, downloader)

    def fetch_jar(snapshot: str) -> dict[str, Download]:
        assert downloader is not None
        return jar_files.archive_register_files(snapshot, archive, jar, downloader)

    # One per process, so that the days a run fetches, in order, are read in one pass.
    vmi_changelog = spinta.Changelog(vmi.register_url, api) if api else None

    def fetch_vmi_daily_changes(snapshot: str) -> dict[str, Download]:
        assert vmi_changelog is not None
        made_from, made_until = vmi_daily_cadence.window_before(snapshot)
        return vmi_daily_changes.archive_changes(
            snapshot, made_from, made_until, archive, vmi_changelog
        )

    def fetch_vmi_monthly_register(snapshot: str) -> dict[str, Download]:
        assert downloader is not None
        return vmi_monthly_register.archive_register(snapshot, archive, vmi, downloader)

    return [
        ArchivedSource(
            name=sodra_files.SOURCE_NAME,
            cadence=SnapshotCadence(
                schedule=sodra.snapshot_schedule,
                key_format=sodra_files.SNAPSHOT_KEY_FORMAT,
                timezone=settings.timezone,
            ),
            fetch=fetch_sodra if downloader else None,
        ),
        ArchivedSource(
            name=jar_files.SOURCE_NAME,
            cadence=SnapshotCadence(
                schedule=jar.snapshot_schedule,
                key_format=jar_files.SNAPSHOT_KEY_FORMAT,
                timezone=settings.timezone,
            ),
            fetch=fetch_jar if downloader else None,
        ),
        ArchivedSource(
            name=vmi_daily_changes.SOURCE_NAME,
            cadence=vmi_daily_cadence,
            fetch=fetch_vmi_daily_changes if api else None,
            history_start=vmi.first_daily_changes,
            group=VMI_DAILY_CHANGES_GROUP,
        ),
        ArchivedSource(
            name=vmi_monthly_register.SOURCE_NAME,
            cadence=SnapshotCadence(
                schedule=vmi.monthly_register_schedule,
                key_format=vmi_monthly_register.SNAPSHOT_KEY_FORMAT,
                timezone=settings.timezone,
            ),
            fetch=fetch_vmi_monthly_register if downloader else None,
            group=VMI_MONTHLY_REGISTER_GROUP,
        ),
    ]


def build_raw_archive(
    mode: PipelineMode, settings: PipelineSettings, storage: LakeStorage
) -> FsspecRawArchive:
    path = raw_archive_path(mode, settings, storage)
    if mode is PipelineMode.LIVE:
        return open_fsspec_archive(filesystem_configuration(path, storage))
    return open_fsspec_archive(FilesystemConfiguration(bucket_url=path))
