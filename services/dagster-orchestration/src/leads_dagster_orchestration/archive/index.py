import json
from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass

import dagster as dg

from leads_config.configuration import Configuration
from leads_dagster_orchestration.archive.metadata import files_listing
from leads_dagster_orchestration.asset_keys import archive_key
from leads_dagster_orchestration.snapshots import ArchivedSource
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.settings import PipelineMode

ARCHIVE_INDEX = "archive_index"


@dataclass(frozen=True, slots=True)
class ArchiveIndexSettings:
    sensor_interval_seconds: int


def load_archive_index_settings(config: Configuration) -> ArchiveIndexSettings:
    interval = config.get_int("archive_index.sensor_interval_seconds")
    if interval < 1:
        raise ValueError(f"'archive_index.sensor_interval_seconds' must be at least 1: {interval}")
    return ArchiveIndexSettings(sensor_interval_seconds=interval)


@dataclass(frozen=True, slots=True)
class IndexChanges:
    added: list[str]
    removed: list[str]
    # New or completed again: each is reported as a materialization of the source's archive.
    completed: list[str]


def index_changes(
    registered: Set[str], seen: Mapping[str, str], current: Mapping[str, str]
) -> IndexChanges:
    """`seen` and `current` map snapshots to manifest versions, at the last tick and now.
    Matching `registered`, Dagster's own, adds back a partition deleted by hand."""
    added = sorted(set(current) - set(registered))
    return IndexChanges(
        added=added,
        removed=sorted(set(registered) - set(current)),
        completed=sorted(
            snapshot
            for snapshot, version in current.items()
            if seen.get(snapshot) != version or snapshot in added
        ),
    )


def build_archive_index(
    sources: Sequence[ArchivedSource],
    archive: RawArchive,
    lake: PipelineMode,
    partitions: Mapping[str, dg.DynamicPartitionsDefinition],
    labels: Mapping[str, str],
    settings: ArchiveIndexSettings,
) -> dg.SensorDefinition:
    """Whoever completed a snapshot (a fetch, the replay sample, a copy by hand), it gets a
    partition. One removed from the archive loses it, but its rows stay in raw."""

    @dg.sensor(
        name=ARCHIVE_INDEX,
        minimum_interval_seconds=settings.sensor_interval_seconds,
        default_status=dg.DefaultSensorStatus.RUNNING,
        description="Adds a partition for each complete snapshot in the archive, and reports it.",
    )
    def archive_index(context: dg.SensorEvaluationContext) -> dg.SensorResult:
        seen: dict[str, dict[str, str]] = json.loads(context.cursor) if context.cursor else {}
        events: list[dg.AssetMaterialization] = []
        requests: list[dg.AddDynamicPartitionsRequest | dg.DeleteDynamicPartitionsRequest] = []
        for source in sources:
            current = archive.snapshots(source.name)
            source_partitions = partitions[source.name]
            registered = set(context.instance.get_dynamic_partitions(source_partitions.name))
            changes = index_changes(registered, seen.get(source.name, {}), current)
            if changes.added:
                requests.append(source_partitions.build_add_request(changes.added))
            if changes.removed:
                requests.append(source_partitions.build_delete_request(changes.removed))
            for snapshot in changes.completed:
                manifest = archive.manifest(source.name, snapshot)
                events.append(
                    dg.AssetMaterialization(
                        asset_key=archive_key(source.name, lake),
                        partition=snapshot,
                        metadata={
                            "completed_at": manifest.completed_at.isoformat(),
                            "files": dg.MetadataValue.json(files_listing(manifest.files)),
                            **labels,
                        },
                    )
                )
            seen[source.name] = current
        return dg.SensorResult(
            asset_events=events,
            dynamic_partitions_requests=requests,
            cursor=json.dumps(seen, sort_keys=True),
        )

    return archive_index
