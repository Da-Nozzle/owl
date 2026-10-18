from collections.abc import Iterator, Mapping
from datetime import UTC, datetime

import dagster as dg

from leads_dagster_orchestration.archive.metadata import (
    files_listing,
    files_metadata,
    plan_metadata,
)
from leads_dagster_orchestration.asset_keys import archive_key, fetch_key, plan_key, source_group
from leads_dagster_orchestration.automation import plan_on_change
from leads_dagster_orchestration.lake_labels import label_run
from leads_dagster_orchestration.snapshots import ArchivedSource
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.settings import PipelineMode

WITHIN_BUDGET = "within_budget"


def build_archive_spec(
    source: ArchivedSource, lake: PipelineMode, partitions: dg.DynamicPartitionsDefinition
) -> dg.AssetSpec:
    """Not materializable, so nothing can write a snapshot for a period that has passed: the
    archive index reports each snapshot as it completes."""
    where = "the raw archive" if lake is PipelineMode.LIVE else "the committed replay sample"
    return dg.AssetSpec(
        key=archive_key(source.name, lake),
        group_name=source_group(source.group_path, lake),
        partitions_def=partitions,
        deps=[fetch_key(source.name, lake)] if source.fetches else [],
        description=(
            f"The {source.name} snapshots in {where}, one partition each, as the archive index "
            f"finds them complete. dbt reads them into the source's raw models."
        ),
    )


def build_fetch_asset(
    source: ArchivedSource, archive: RawArchive, labels: Mapping[str, str]
) -> dg.AssetsDefinition:
    """Unpartitioned, as the clock picks the snapshot, unless the source keeps its history: then
    partitioned by period, a backfill fetching its periods in order in one run."""
    fetch = source.fetch
    if fetch is None:
        raise ValueError(f"Source {source.name!r} has no fetch, so it has no fetch asset")
    key = fetch_key(source.name, PipelineMode.LIVE)
    periods = source.fetch_periods()

    @dg.asset(
        key=key,
        group_name=source_group(source.group_path, PipelineMode.LIVE),
        pool=fetch_pool(source),
        deps=source.fetch_deps,
        partitions_def=periods,
        backfill_policy=dg.BackfillPolicy.single_run() if periods else None,
        description=(
            "Fetches the source into the raw archive as the current period's snapshot (or, for a "
            "source with history, each of the partitions'), then marks each complete as it is "
            "fetched; the archive index then registers it, and raw and staging build from it."
        ),
    )
    def fetch_snapshot(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        run_labels = label_run(context, labels, fetches=True)
        snapshots = (
            sorted(context.partition_keys)
            if periods
            else [source.cadence.snapshot_at(datetime.now(UTC))]
        )
        fetched = {}
        for snapshot in snapshots:
            downloads = fetch(snapshot)
            archive.complete(source.name, snapshot, downloads, datetime.now(UTC))
            fetched[snapshot] = files_listing(files_metadata(downloads))
        first, last = snapshots[0], snapshots[-1]
        return dg.MaterializeResult(
            metadata={
                "snapshot": first if first == last else f"{first} to {last}",
                # A run of several snapshots lists each one's files.
                "files": dg.MetadataValue.json(fetched[first] if first == last else fetched),
                **run_labels,
            }
        )

    return fetch_snapshot


def fetch_pool(source: ArchivedSource) -> str:
    """Two fetches of a source never write a snapshot at once, nor does its plan read the API
    beside its fetch."""
    return f"{PipelineMode.LIVE.value}_{fetch_key(source.name, PipelineMode.LIVE).path[-1]}"


def build_planned_fetch_asset(
    source: ArchivedSource, archive: RawArchive, labels: Mapping[str, str]
) -> dg.AssetsDefinition:
    """A paid source's plan and fetch in one step, so a fetch pays for the plan made just before
    it. Selected alone, the fetch still plans first, without recording the plan."""
    plan = source.plan
    if plan is None:
        raise ValueError(f"Source {source.name!r} has no plan, so it has no planned fetch")
    if source.history_start is not None:
        raise ValueError(f"Source {source.name!r} keeps its history: a plan takes no period")
    group = source_group(source.group_path, PipelineMode.LIVE)
    planned = plan_key(source.name, PipelineMode.LIVE)
    fetched = fetch_key(source.name, PipelineMode.LIVE)
    budget = dg.AssetCheckKey(planned, WITHIN_BUDGET)

    @dg.multi_asset(
        name=f"{PipelineMode.LIVE.value}_plan_and_fetch_{source.name}",
        specs=[
            dg.AssetSpec(
                planned,
                group_name=group,
                deps=source.fetch_deps,
                automation_condition=plan_on_change(),
                skippable=True,
                description=(
                    f"What {source.name}'s fetch pays for, planned for free and priced against "
                    f"its budget. Planned again whenever what it reads changes."
                ),
            ),
            dg.AssetSpec(
                fetched,
                group_name=group,
                deps=[planned],
                skippable=True,
                description=(
                    f"Pays for {source.name}'s plan, made in the same step, as the current "
                    f"period's snapshot in the raw archive."
                ),
            ),
        ],
        check_specs=[
            dg.AssetCheckSpec(
                WITHIN_BUDGET,
                asset=planned,
                blocking=True,
                description="The plan's paid requests are at most the most a fetch may make.",
            )
        ],
        can_subset=True,
        pool=fetch_pool(source),
        description=(
            "Plans the source's next fetch for free, records the plan and its check if they are "
            "selected, then, if the fetch is, pays for that plan."
        ),
    )
    def plan_and_fetch(
        context: dg.AssetExecutionContext,
    ) -> Iterator[dg.MaterializeResult | dg.AssetCheckResult]:
        fetches = fetched in context.selected_asset_keys
        run_labels = label_run(context, labels, fetches=fetches)
        made = plan()
        if planned in context.selected_asset_keys:
            yield dg.MaterializeResult(
                asset_key=planned, metadata={**plan_metadata(made), **run_labels}
            )
        if budget in context.selected_asset_check_keys:
            yield dg.AssetCheckResult(
                asset_key=planned,
                check_name=WITHIN_BUDGET,
                passed=made.within_budget,
                metadata={
                    "paid_requests": dg.MetadataValue.int(made.paid_requests),
                    "max_paid_requests": dg.MetadataValue.int(made.max_paid_requests),
                },
            )
        if not fetches:
            return
        if not made.within_budget:
            raise dg.Failure(
                f"The plan pays for {made.paid_requests:,} requests, more than the "
                f"{made.max_paid_requests:,} a fetch may make; nothing was paid"
            )
        snapshot = source.cadence.snapshot_at(datetime.now(UTC))
        downloads = made.pay(snapshot)
        archive.complete(source.name, snapshot, downloads, datetime.now(UTC))
        yield dg.MaterializeResult(
            asset_key=fetched,
            metadata={
                "snapshot": snapshot,
                "files": dg.MetadataValue.json(files_listing(files_metadata(downloads))),
                **run_labels,
            },
        )

    return plan_and_fetch
