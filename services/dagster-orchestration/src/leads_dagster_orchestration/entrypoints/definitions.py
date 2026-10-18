from collections.abc import Callable, Sequence
from pathlib import Path

import dagster as dg
from dagster_dbt import DbtProject

from leads_config.configuration import Configuration
from leads_config.loading import load_configuration
from leads_dagster_orchestration.archive_index import build_archive_index
from leads_dagster_orchestration.asset_keys import plan_key, transform_model_key
from leads_dagster_orchestration.assets.archive import (
    build_archive_spec,
    build_fetch_asset,
    build_planned_fetch_asset,
)
from leads_dagster_orchestration.assets.transform import (
    build_transform_assets,
    ensure_metadata_database,
    prepare_on_initial_load,
    set_dbt_environment,
)
from leads_dagster_orchestration.automation import (
    plan_automation_sensor,
    transform_automation_sensor,
)
from leads_dagster_orchestration.dbt_resource import dbt_core_resource
from leads_dagster_orchestration.executors import sequential_in_process_executor
from leads_dagster_orchestration.jobs import fetch_job, fetch_schedule, rebuild_job
from leads_dagster_orchestration.lake_labels import lake_labels
from leads_dagster_orchestration.lineage import build_lineage_sensor, load_lineage_settings
from leads_dagster_orchestration.snapshots import (
    ArchivedSource,
    FetchPlan,
    SnapshotCadence,
    snapshot_partitions,
)
from leads_dagster_orchestration.tool_environments import (
    dagster_dbt_environment,
    dbt_environment,
    dbt_main_state_dir,
)
from leads_ingest.adapters.archive.fsspec_archive import FsspecRawArchive, open_fsspec_archive
from leads_ingest.adapters.ducklake.reader import DuckLakeReader
from leads_ingest.adapters.http.client import build_http_client
from leads_ingest.adapters.http.downloader import HttpDownloader
from leads_ingest.adapters.http.places_api import HttpPlacesApi
from leads_ingest.adapters.http.settings import load_http_client_settings
from leads_ingest.adapters.s3.configuration import s3fs_options
from leads_ingest.ports.downloader import Download, Downloader
from leads_ingest.ports.lake_reader import LakeReader
from leads_ingest.ports.places_api import PlacesApi
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.settings import (
    PipelineMode,
    PipelineSettings,
    load_pipeline_settings,
    raw_archive_path,
)
from leads_ingest.sources import spinta
from leads_ingest.sources.address_register import files as address_register_files
from leads_ingest.sources.address_register.settings import load_address_register_settings
from leads_ingest.sources.google_places import areas as google_places_areas
from leads_ingest.sources.google_places import text_search as google_places_text_search
from leads_ingest.sources.google_places.settings import load_google_places_settings
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
# Google's listings are one box, `google`, holding the backend they are fetched through (the
# Places API, `google_places`) and the staging model in models/staging/google/, which a second
# backend, such as a scraper, would feed too.
GOOGLE_GROUP = "google"
GOOGLE_PLACES_GROUP = f"{GOOGLE_GROUP}/places"
# Where Google is searched: the dbt model Google's plan reads.
GOOGLE_SEARCH_AREAS = transform_model_key(
    google_places_areas.AREAS_SCHEMA, google_places_areas.AREAS_MODEL, PipelineMode.LIVE
)


# TODO split up this file, too much is in one place
def pipeline_definitions(mode: PipelineMode, config: Configuration | None = None) -> dg.Definitions:
    """One lake's code location (entrypoints/live.py, entrypoints/replay.py), built from its
    archived sources. Per source: its archive, partitioned by the snapshots the archive holds
    (the archive index adds them); its dbt models, raw partitioned the same way; and a job to
    rebuild from the archive. Live adds a fetch per source, with its job and schedule, and for a
    source billed per request the plan it pays for, made in the same step; replay's archive is
    the committed sample, which nothing fetches into. A new snapshot builds itself."""
    config = config or load_configuration()
    pipeline_settings = load_pipeline_settings(config)
    lake_settings = load_lake_settings(config, replay=mode is PipelineMode.REPLAY)
    archive = build_raw_archive(mode, pipeline_settings, lake_settings.storage)
    labels = lake_labels(mode, lake_settings)
    live = mode is PipelineMode.LIVE
    user_agent = pipeline_settings.user_agent

    # Each source sends its requests through a client of its own, configured by its table
    # (`<source>.http`: timeout, retries, spacing), read here, so a malformed one fails the load.
    def downloader(key: str) -> Downloader:
        return HttpDownloader(build_http_client(user_agent, load_http_client_settings(config, key)))

    places_client = (
        build_http_client(user_agent, load_http_client_settings(config, "google_places.http"))
        if live
        else None
    )

    def places_api() -> PlacesApi:
        # Google's key is read when a fetch first sends a request, so loading the definitions
        # reads no secret, and a missing key fails only Google's fetch.
        assert places_client is not None
        return HttpPlacesApi(
            client=places_client,
            api_key=config.get_str("google_places.api_key"),
            search_text_url=config.get_str("google_places.search_text_url"),
        )

    # The lake the live fetches read models of (Google's search areas), read-only.
    lake_reader = (
        DuckLakeReader(lake_settings, config.get_str("transform.duckdb_memory_limit"))
        if live
        else None
    )
    sources = archived_sources(
        config,
        pipeline_settings,
        archive,
        downloader if live else None,
        places_api if live else None,
        lake_reader,
    )

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
            build_archive_index(sources, archive, mode, partitions, labels),
            transform_automation_sensor(transform_assets),
            *(
                plan_automation_sensor(source, plan_key(source.name, PipelineMode.LIVE))
                for source in planned
            ),
            build_lineage_sensor(
                load_lineage_settings(config), user_agent, pipeline_settings.timezone
            ),
        ],
        resources={"dbt": dbt_core_resource(transform_project)},
        executor=sequential_in_process_executor,
    )


def archived_sources(
    config: Configuration,
    settings: PipelineSettings,
    archive: RawArchive,
    downloaders: Callable[[str], Downloader] | None,
    places_api: Callable[[], PlacesApi] | None = None,
    lake_reader: LakeReader | None = None,
) -> Sequence[ArchivedSource]:
    """Every source that goes through the archive. A source is one entry here, its fetch (used
    only with `downloaders`, in the live lake: `downloaders` makes a source's downloader from its
    client's table, `places_api` the client of Google's Places API, and `lake_reader` reads the
    models a fetch reads) and its dbt models in models/raw/<name>/ and models/staging/<name>/."""
    sodra = load_sodra_settings(config)
    jar = load_jar_settings(config)
    address_register = load_address_register_settings(config)
    vmi = load_vmi_settings(config)
    google_places = load_google_places_settings(config)
    vmi_daily_cadence = SnapshotCadence(
        schedule=vmi.daily_changes_schedule,
        key_format=vmi_daily_changes.SNAPSHOT_KEY_FORMAT,
        timezone=settings.timezone,
    )

    def downloader(key: str) -> Downloader | None:
        return downloaders(key) if downloaders else None

    sodra_downloader = downloader("sodra.http")
    jar_downloader = downloader("jar.http")
    address_register_downloader = downloader("address_register.http")
    vmi_changelog_downloader = downloader("vmi.daily_changes_http")
    vmi_register_downloader = downloader("vmi.monthly_register_http")

    def fetch_sodra(snapshot: str) -> dict[str, Download]:
        assert sodra_downloader is not None
        return sodra_files.archive_yearly_files(snapshot, archive, sodra, sodra_downloader)

    def fetch_jar(snapshot: str) -> dict[str, Download]:
        assert jar_downloader is not None
        return jar_files.archive_register_files(snapshot, archive, jar, jar_downloader)

    def fetch_address_register(snapshot: str) -> dict[str, Download]:
        assert address_register_downloader is not None
        return address_register_files.archive_register_files(
            snapshot, archive, address_register, address_register_downloader
        )

    # One per process, so that the days a run fetches, in order, are read in one pass.
    vmi_changelog = (
        spinta.Changelog(vmi.register_url, vmi_changelog_downloader)
        if vmi_changelog_downloader
        else None
    )

    def fetch_vmi_daily_changes(snapshot: str) -> dict[str, Download]:
        assert vmi_changelog is not None
        made_from, made_until = vmi_daily_cadence.window_before(snapshot)
        return vmi_daily_changes.archive_changes(
            snapshot, made_from, made_until, archive, vmi_changelog
        )

    def fetch_vmi_monthly_register(snapshot: str) -> dict[str, Download]:
        assert vmi_register_downloader is not None
        return vmi_monthly_register.archive_register(
            snapshot, archive, vmi, vmi_register_downloader
        )

    def google_search_areas() -> list[google_places_areas.SearchArea]:
        assert lake_reader is not None
        return google_places_areas.read_search_areas(lake_reader)

    def plan_google_places() -> FetchPlan:
        assert places_api is not None
        client = places_api()
        plan = google_places_text_search.plan_fetch(google_places, client, google_search_areas())

        def pay(snapshot: str) -> dict[str, Download]:
            return google_places_text_search.pay_for_plan(
                snapshot, archive, google_places, client, plan
            )

        return google_places_fetch_plan(plan, pay)

    return [
        ArchivedSource(
            name=sodra_files.SOURCE_NAME,
            cadence=SnapshotCadence(
                schedule=sodra.snapshot_schedule,
                key_format=sodra_files.SNAPSHOT_KEY_FORMAT,
                timezone=settings.timezone,
            ),
            fetch=fetch_sodra if sodra_downloader else None,
        ),
        ArchivedSource(
            name=jar_files.SOURCE_NAME,
            cadence=SnapshotCadence(
                schedule=jar.snapshot_schedule,
                key_format=jar_files.SNAPSHOT_KEY_FORMAT,
                timezone=settings.timezone,
            ),
            fetch=fetch_jar if jar_downloader else None,
        ),
        ArchivedSource(
            name=address_register_files.SOURCE_NAME,
            cadence=SnapshotCadence(
                schedule=address_register.snapshot_schedule,
                key_format=address_register_files.SNAPSHOT_KEY_FORMAT,
                timezone=settings.timezone,
            ),
            fetch=fetch_address_register if address_register_downloader else None,
        ),
        ArchivedSource(
            name=vmi_daily_changes.SOURCE_NAME,
            cadence=vmi_daily_cadence,
            fetch=fetch_vmi_daily_changes if vmi_changelog else None,
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
            fetch=fetch_vmi_monthly_register if vmi_register_downloader else None,
            group=VMI_MONTHLY_REGISTER_GROUP,
        ),
        ArchivedSource(
            name=google_places_text_search.SOURCE_NAME,
            cadence=SnapshotCadence(
                schedule=google_places.snapshot_schedule,
                key_format=google_places_text_search.SNAPSHOT_KEY_FORMAT,
                timezone=settings.timezone,
            ),
            group=GOOGLE_PLACES_GROUP,
            fetch_max_runtime_seconds=google_places_text_search.FETCH_MAX_RUNTIME_SECONDS,
            # Every sweep is paid for: turn the schedule on once the first sweep was wanted.
            schedule_running=False,
            fetch_deps=(GOOGLE_SEARCH_AREAS,),
            plan=plan_google_places if places_api and lake_reader else None,
        ),
    ]


def build_raw_archive(
    mode: PipelineMode, settings: PipelineSettings, storage: LakeStorage
) -> FsspecRawArchive:
    path = raw_archive_path(mode, settings, storage)
    if mode is PipelineMode.LIVE:
        return open_fsspec_archive(path, s3fs_options(storage))
    return open_fsspec_archive(path)


def google_places_fetch_plan(
    plan: google_places_text_search.SweepPlan, pay: Callable[[str], dict[str, Download]]
) -> FetchPlan:
    """Google's sweep planned as Dagster shows a plan: a row per search area, and the next
    fetch's requests in USD, all billed and beyond the month's free ones."""
    free = google_places_text_search.FREE_PAID_REQUESTS_A_MONTH
    usd = google_places_text_search.USD_PER_PAID_REQUEST
    paid = plan.paid_requests
    return FetchPlan(
        paid_requests=paid,
        max_paid_requests=plan.max_paid_requests,
        summary=(
            f"The next fetch pays for {paid:,} requests: USD {paid * usd:,.2f} if all are "
            f"billed, USD {max(paid - free, 0) * usd:,.2f} beyond the {free:,} free a month. "
            f"A fetch may pay for {plan.max_paid_requests:,}."
        ),
        pay=pay,
        parts=[
            {
                "municipality": area.area.name,
                "code": area.area.code,
                "mode": area.area.mode.value,
                "places": area.places,
                "free requests": area.free_requests,
                "paid requests alone": area.paid_requests,
                "USD alone if billed": f"{area.paid_requests * usd:,.2f}",
            }
            for area in plan.areas
        ],
    )
