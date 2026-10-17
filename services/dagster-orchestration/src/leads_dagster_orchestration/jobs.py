import dagster as dg

# What define_asset_job and build_schedule_from_partitioned_job return; dagster exports neither.
from dagster._core.definitions.partitions.partitioned_schedule import (
    UnresolvedPartitionedAssetScheduleDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from leads_dagster_orchestration.asset_keys import archive_key, fetch_key
from leads_dagster_orchestration.snapshots import ArchivedSource
from leads_ingest.settings import PipelineMode

# Each source has two jobs, so nobody picks assets to fetch or to rebuild: fetch_<source> (live
# only) captures a snapshot, and rebuild_<source>_from_archive rebuilds from snapshots already
# archived. Building a new snapshot is neither: Dagster does that once the archive index finds
# it (automation.py).


def fetch_job_name(source: str) -> str:
    return f"fetch_{source}"


def rebuild_job_name(source: str) -> str:
    return f"rebuild_{source}_from_archive"


def fetch_job(source: ArchivedSource) -> UnresolvedAssetJobDefinition:
    what = (
        "the chosen periods (the current one on its schedule)"
        if source.history_start
        else "as it is now, as the current period's snapshot"
    )
    return dg.define_asset_job(
        fetch_job_name(source.name),
        selection=dg.AssetSelection.assets(fetch_key(source.name, PipelineMode.LIVE)),
        description=(
            f"Fetches {source.name} into the archive: {what}. Raw and staging then build from "
            f"it on their own."
        ),
    )


def fetch_schedule(
    source: ArchivedSource, job: UnresolvedAssetJobDefinition
) -> dg.ScheduleDefinition | UnresolvedPartitionedAssetScheduleDefinition:
    """Fetches the source on its own cadence. Running unless turned off, as it is the live
    pipeline. A source with history fetches the period that begins at the tick, the one the
    clock would pick, as its partition."""
    name = f"{fetch_job_name(source.name)}_schedule"
    if source.history_start:
        return dg.build_schedule_from_partitioned_job(
            job, name=name, default_status=dg.DefaultScheduleStatus.RUNNING
        )
    return dg.ScheduleDefinition(
        name=name,
        job=job,
        cron_schedule=source.cadence.schedule,
        execution_timezone=source.cadence.timezone,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )


def rebuild_job(source: ArchivedSource, lake: PipelineMode) -> UnresolvedAssetJobDefinition:
    """Everything after the source's archive, for the snapshots chosen: the job for a change to
    how raw reads the archive. It fetches nothing, and offers only snapshots that exist."""
    return dg.define_asset_job(
        rebuild_job_name(source.name),
        selection=dg.AssetSelection.assets(archive_key(source.name, lake)).downstream(
            include_self=False
        ),
        description=(
            f"Rebuilds {source.name}'s raw for the chosen snapshots from the archive, then "
            f"staging and everything after it once. Fetches nothing."
        ),
    )
