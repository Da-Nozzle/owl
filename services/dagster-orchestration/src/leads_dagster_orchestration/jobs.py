import dagster as dg

# What define_asset_job and build_schedule_from_partitioned_job return; dagster exports neither.
from dagster._core.definitions.partitions.partitioned_schedule import (
    UnresolvedPartitionedAssetScheduleDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from leads_dagster_orchestration.asset_keys import (
    FETCH_LAYER,
    PLAN_LAYER,
    archive_key,
    fetch_key,
    plan_key,
)
from leads_dagster_orchestration.snapshots import MAX_RUNTIME_TAG, ArchivedSource
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
    """Fetches the source; for a paid source, plans it and pays for that plan, in one step, with
    the plan recorded and checked against the budget."""
    what = (
        "the chosen periods (the current one on its schedule)"
        if source.history_start
        else "as it is now, as the current period's snapshot"
    )
    if source.plan is not None:
        what += ", paying for the plan made in the same step, if it is within the budget"
    keys = [fetch_key(source.name, PipelineMode.LIVE)]
    if source.plan is not None:
        keys.append(plan_key(source.name, PipelineMode.LIVE))
    runtime = source.fetch_max_runtime_seconds
    return dg.define_asset_job(
        fetch_job_name(source.name),
        selection=dg.AssetSelection.assets(*keys),
        description=(
            f"Fetches {source.name} into the archive: {what}. Raw and staging then build from "
            f"it on their own."
        ),
        tags={MAX_RUNTIME_TAG: str(runtime)} if runtime else None,
    )


def fetch_schedule(
    source: ArchivedSource, job: UnresolvedAssetJobDefinition
) -> dg.ScheduleDefinition | UnresolvedPartitionedAssetScheduleDefinition:
    """Fetches the source on its own cadence. Running unless turned off, as it is the live
    pipeline, except for a source that starts stopped (`schedule_running`). A source with
    history fetches the period that begins at the tick, the one the clock would pick, as its
    partition."""
    name = f"{fetch_job_name(source.name)}_schedule"
    status = (
        dg.DefaultScheduleStatus.RUNNING
        if source.schedule_running
        else dg.DefaultScheduleStatus.STOPPED
    )
    if source.history_start:
        return dg.build_schedule_from_partitioned_job(job, name=name, default_status=status)
    return dg.ScheduleDefinition(
        name=name,
        job=job,
        cron_schedule=source.cadence.schedule,
        execution_timezone=source.cadence.timezone,
        default_status=status,
    )


def rebuild_job(source: ArchivedSource, lake: PipelineMode) -> UnresolvedAssetJobDefinition:
    """Everything after the source's archive, for the snapshots chosen: the job for a change to
    how raw reads the archive. It offers only snapshots that exist, and it fetches and plans
    nothing: Google's plan reads a model built from Sodra's staging, so it is downstream of
    Sodra's archive, and a rebuild of Sodra must not pay for a sweep. What such a fetch or plan
    leads to (Google's fetch, archive, raw and staging) is left out with it; a model that also reads
    this source rebuilds itself once this source's staging has (rebuild_on_change)."""
    reached = dg.AssetSelection.assets(archive_key(source.name, lake)).downstream(
        include_self=False
    )
    paid = reached & dg.AssetSelection.key_prefixes(
        [lake.value, FETCH_LAYER], [lake.value, PLAN_LAYER]
    )
    return dg.define_asset_job(
        rebuild_job_name(source.name),
        selection=reached - paid.downstream(include_self=True),
        description=(
            f"Rebuilds {source.name}'s raw for the chosen snapshots from the archive, then "
            f"staging and everything after it once. Fetches nothing."
        ),
    )
