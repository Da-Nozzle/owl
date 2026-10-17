from collections.abc import Mapping
from datetime import UTC, datetime

import dagster as dg

from leads_dagster_orchestration.asset_keys import archive_key, fetch_key, source_group
from leads_dagster_orchestration.lake_labels import label_run
from leads_dagster_orchestration.snapshots import ArchivedSource
from leads_ingest.ports.downloader import Download
from leads_ingest.ports.raw_archive import RawArchive, file_record
from leads_ingest.settings import PipelineMode


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
        deps=[fetch_key(source.name, lake)] if source.fetch else [],
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
        pool=f"{PipelineMode.LIVE.value}_{key.path[-1]}",
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
            fetched[snapshot] = files_metadata(downloads)
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


def files_metadata(downloads: Mapping[str, Download]) -> dict[str, dict[str, object]]:
    return {name: file_record(download) for name, download in downloads.items()}
