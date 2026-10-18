import dagster as dg

# Dagster exports neither type its job and schedule builders return.
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
from leads_dagster_orchestration.run_tags import MAX_RUNTIME_TAG
from leads_dagster_orchestration.snapshots import ArchivedSource
from leads_ingest.settings import PipelineMode


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
    """Everything after the source's archive, except any fetch or plan and what follows them:
    Google's plan reads Sodra's staging, and a rebuild of Sodra must not pay for a sweep."""
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
