from collections.abc import Callable, Mapping
from datetime import UTC, datetime

import dagster as dg

from leads_dagster_orchestration.asset_keys import archive_key, layer_group
from leads_dagster_orchestration.lake_labels import label_run
from leads_ingest.months import Month
from leads_ingest.ports.downloader import Download
from leads_ingest.sources.sodra.files import SOURCE_NAME


def build_sodra_archive_asset(
    partitions: dg.MonthlyPartitionsDefinition,
    archive_files_for: Callable[[Month], Mapping[str, Download]],
    labels: Mapping[str, str],
    *,
    downloads: bool,
) -> dg.AssetsDefinition:
    key = archive_key(SOURCE_NAME)

    @dg.asset(key=key, group_name=layer_group(key), partitions_def=partitions)
    def sodra_archive(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        """Sodra's yearly files covering the run month's window, as downloaded, in the raw
        archive (replay: the committed sample). dbt reads them into raw_sodra__monthly."""
        label_run(context, labels)
        if downloads:
            require_current_run_month(partitions, context.partition_key, datetime.now(UTC))
        archived = archive_files_for(Month.parse(context.partition_key))
        # TODO record each file's S3 VersionId (RawArchive.version_id), so a run month links to
        # the exact bytes its build read; it needs the archive bucket versioned.
        return dg.MaterializeResult(
            metadata={"files": dg.MetadataValue.json(files_metadata(archived)), **labels}
        )

    return sodra_archive


def files_metadata(archived: Mapping[str, Download]) -> dict[str, dict[str, object]]:
    """Each archived file's fetch, by name: two run months whose files have the same sha256 hold
    the same version of them."""
    return {
        name: {
            "url": download.url,
            "fetched_at": download.fetched_at.isoformat(),
            "last_modified": download.last_modified,
            "size": download.size,
            "sha256": download.sha256,
        }
        for name, download in archived.items()
    }


def require_current_run_month(
    partitions: dg.MonthlyPartitionsDefinition, run_month: str, now: datetime
) -> None:
    """Sodra rewrites its files in place, so a download is the version of the day it is made,
    whichever run month it is for. Downloading for a run month that has passed would archive
    today's files as that month's, and staging would take them for that month's version. Only
    the current run month downloads; a past one rebuilds from what it archived then."""
    current = partitions.get_partition_key_for_timestamp(now.timestamp())
    if run_month != current:
        raise dg.Failure(
            description=(
                f"Run month {run_month} has passed (the current one is {current}): Sodra has "
                f"replaced the files it published then, so downloading them now would archive "
                f"today's files as {run_month}'s. To rebuild {run_month} from its archived "
                f"files, materialize only the assets downstream of "
                f"{archive_key(SOURCE_NAME).to_user_string()}."
            )
        )


def build_sodra_archive_spec(partitions: dg.MonthlyPartitionsDefinition) -> dg.AssetSpec:
    """The replay archive as an external asset: the committed sample is read, never fetched
    into, so the graph keeps the archive node without reporting a materialization that wrote
    nothing."""
    # TODO return dg.AssetSpec(key=archive_key(SOURCE_NAME), group_name=..., partitions_def=...)
    # and use it in place of build_sodra_archive_asset in replay mode.
    raise NotImplementedError
