from collections.abc import Callable

import dagster as dg

from leads_dagster_orchestration.asset_keys import archive_key, layer_group
from leads_ingest.months import Month
from leads_ingest.sources.sodra.files import SOURCE_NAME


def build_sodra_archive_asset(
    partitions: dg.MonthlyPartitionsDefinition,
    archive_files_for: Callable[[Month], list[str]],
) -> dg.AssetsDefinition:
    key = archive_key(SOURCE_NAME)

    @dg.asset(key=key, group_name=layer_group(key), partitions_def=partitions)
    def sodra_archive(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        """Sodra's yearly files covering the run month's window, as downloaded, in the raw
        archive (replay: the committed sample). dbt reads them into raw_sodra__monthly."""
        names = archive_files_for(Month.parse(context.partition_key))
        # TODO record each file's S3 VersionId (RawArchive.version_id), fetch time,
        # Last-Modified and size, so a run month links to the exact bytes its build read.
        return dg.MaterializeResult(metadata={"files": names})

    return sodra_archive


def build_sodra_archive_spec(partitions: dg.MonthlyPartitionsDefinition) -> dg.AssetSpec:
    """The replay archive as an external asset: the committed sample is read, never fetched
    into, so the graph keeps the archive node without reporting a materialization that wrote
    nothing."""
    # TODO return dg.AssetSpec(key=archive_key(SOURCE_NAME), group_name=..., partitions_def=...)
    # and use it in place of build_sodra_archive_asset in replay mode.
    raise NotImplementedError
