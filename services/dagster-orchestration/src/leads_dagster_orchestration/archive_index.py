import json
from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass

import dagster as dg

from leads_dagster_orchestration.asset_keys import archive_key
from leads_dagster_orchestration.snapshots import ArchivedSource
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.settings import PipelineMode

ARCHIVE_INDEX = "archive_index"


@dataclass(frozen=True, slots=True)
class IndexChanges:
    """How a source's partitions and archive asset catch up with its complete snapshots."""

    added: list[str]  # partitions to add: snapshots completed since the last look
    removed: list[str]  # partitions to delete: snapshots no longer in the archive
    completed: list[str]  # snapshots to report as archive materializations: new or redone


def index_changes(
    registered: Set[str], seen: Mapping[str, str], current: Mapping[str, str]
) -> IndexChanges:
    """`current` maps each complete snapshot to its manifest's version and `seen` is the same as
    of the last look, so a snapshot completed again (a second fetch in its period) is reported
    again and its raw rebuilt. Partitions are matched against those Dagster holds, not the last
    look, so partitions deleted by hand or lost with the instance are added back."""
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
) -> dg.SensorDefinition:
    """Keeps each source's partitions the same as its complete snapshots in the lake's archive,
    whoever completed them: a fetch, the committed replay sample, or a copy put there by hand.
    Each new or redone snapshot is reported as a materialization of the source's archive, which
    is what raw builds on. A snapshot removed from the archive (expired, say) loses its
    partition; its rows stay in raw until raw is rebuilt from nothing."""

    @dg.sensor(
        name=ARCHIVE_INDEX,
        minimum_interval_seconds=60,
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
                            "files": dg.MetadataValue.json(dict(manifest.files)),
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
