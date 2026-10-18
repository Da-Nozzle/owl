from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime

import dagster as dg

from leads_dagster_orchestration.asset_keys import archive_key, fetch_key, plan_key, source_group
from leads_dagster_orchestration.automation import plan_on_change
from leads_dagster_orchestration.lake_labels import label_run
from leads_dagster_orchestration.snapshots import ArchivedSource, FetchPlan
from leads_ingest.ports.downloader import Download
from leads_ingest.ports.raw_archive import RawArchive, file_record
from leads_ingest.settings import PipelineMode

# Dagster's metadata lists a snapshot's files, each with its fetch, up to this many. A Places
# sweep is a thousand pages or more, about half a megabyte of records stored with its fetch's
# event and again with the archive's; for a snapshot that large the metadata gives the count and
# size, and the snapshot's manifest in the archive lists each file.
LISTED_FILES_LIMIT = 100
# The check on a paid source's plan: its paid requests are at most the most a fetch may make.
WITHIN_BUDGET = "within_budget"


def build_archive_spec(
    source: ArchivedSource, lake: PipelineMode, partitions: dg.DynamicPartitionsDefinition
) -> dg.AssetSpec:
    """A source's archive: its complete snapshots, one partition each. Nothing materializes it,
    in either lake, so no job or button can write a snapshot for a period that has passed; the
    archive index reports each snapshot as it is completed. In live its fetch is upstream."""
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
    """Captures a source as it is now, as the snapshot of the current period, and completes the
    snapshot. It takes no partition: what it captures is decided by the clock, so a period that
    has passed cannot be fetched again. A source that keeps its history (`history_start`) is the
    exception: its fetch is partitioned by the periods from there to the current one, and fetches
    those asked for, in order and in one run, so that a backfill of a month of days reads the
    source in one pass. In its own pool, so two fetches of a source never write a snapshot at
    once."""
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
    )
    def fetch_snapshot(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        """Fetches the source into the raw archive as the current period's snapshot (or, for a
        source with history, each of the partitions'), then marks each complete as it is
        fetched; the archive index then registers it, and raw and staging build from it."""
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
    """A source's fetch in a pool of its own: two fetches never write one snapshot at once, and
    a paid source's plan never reads the API beside a fetch of the same source."""
    return f"{PipelineMode.LIVE.value}_{fetch_key(source.name, PipelineMode.LIVE).path[-1]}"


def build_planned_fetch_asset(
    source: ArchivedSource, archive: RawArchive, labels: Mapping[str, str]
) -> dg.AssetsDefinition:
    """A paid source's plan and fetch: two assets made in one step, so that a fetch pays for the
    plan made just before it, and no plan it pays for is out of date.

    The plan (plan_<source>) is what the fetch pays for, found without paying, shown as its
    metadata: the paid requests against the most a fetch may make, and the parts they are made
    of. Its check `within_budget` passes when they are at most that. It is planned again by
    itself whenever what it reads changes (`fetch_deps`), so a change to Google's search areas
    is priced before anyone pays for it; that run selects the plan alone and pays for nothing.

    The fetch (fetch_<source>) pays for the plan as the current period's snapshot and completes
    it; its job selects both. It fails before paying for anything over the budget. Selected
    alone, it plans first all the same, without recording the plan. Takes no partition, as a
    source with history would; in the fetch's pool."""
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
    )
    def plan_and_fetch(
        context: dg.AssetExecutionContext,
    ) -> Iterator[dg.MaterializeResult | dg.AssetCheckResult]:
        """Plans the source's next fetch for free, records the plan and its check if they are
        selected, then, if the fetch is, pays for that plan."""
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


def plan_metadata(plan: FetchPlan) -> dict[str, dg.MetadataValue]:
    return {
        "paid_requests": dg.MetadataValue.int(plan.paid_requests),
        "max_paid_requests": dg.MetadataValue.int(plan.max_paid_requests),
        "summary": dg.MetadataValue.md(plan.summary),
        "parts": dg.MetadataValue.md(markdown_table(plan.parts)),
    }


def markdown_table(rows: Sequence[Mapping[str, object]]) -> str:
    if not rows:
        return "Nothing to plan."
    columns = list(rows[0])
    lines = [
        "| " + " | ".join(columns) + " |",
        "|" + "|".join("---" for _ in columns) + "|",
        *("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |" for row in rows),
    ]
    return "\n".join(lines)


def files_metadata(downloads: Mapping[str, Download]) -> dict[str, dict[str, object]]:
    return {name: file_record(download) for name, download in downloads.items()}


def files_listing(files: Mapping[str, Mapping[str, object]]) -> dict[str, object]:
    """A snapshot's files (by name, each with its fetch) as Dagster's metadata shows them: each
    one, or for more than LISTED_FILES_LIMIT, how many and their size in bytes."""
    if len(files) <= LISTED_FILES_LIMIT:
        return dict(files)
    return {
        "file_count": len(files),
        "size": sum(int(str(record["size"])) for record in files.values()),
        "listed_in": "the snapshot's manifest, <archive>/<source>/_snapshots/<snapshot>.json",
    }
