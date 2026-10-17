import dagster as dg

# What define_asset_job returns; dagster does not export it.
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
    return dg.define_asset_job(
        fetch_job_name(source.name),
        selection=dg.AssetSelection.assets(fetch_key(source.name, PipelineMode.LIVE)),
        description=(
            f"Fetches {source.name} as it is now into the archive, as the current period's "
            f"snapshot. Raw and staging then build from it on their own."
        ),
    )


def fetch_schedule(
    source: ArchivedSource, job: UnresolvedAssetJobDefinition
) -> dg.ScheduleDefinition:
    """Fetches the source on its own cadence. Running unless turned off, as it is the live
    pipeline."""
    return dg.ScheduleDefinition(
        name=f"{fetch_job_name(source.name)}_schedule",
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
