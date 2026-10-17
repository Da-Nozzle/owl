from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime

import dagster as dg

from leads_ingest.ports.downloader import Download
from leads_ingest.settings import PipelineMode

# Any time before the first snapshot of any source: where a cadence's calendar starts counting.
CALENDAR_START = datetime(2000, 1, 1)


@dataclass(frozen=True, slots=True)
class SnapshotCadence:
    """When a source is fetched, and how its snapshots are keyed: by the period from one tick of
    `schedule` (cron, in `timezone`) to the next, formatted with `key_format`. A fetch at any time
    is the snapshot of the period it falls in, so a second fetch in a period replaces that
    period's snapshot. Sodra is fetched on the 21st at 06:00 and keyed YYYY-MM; a source fetched
    daily might be `0 3 * * *`, keyed YYYY-MM-DD. How often is the source's own, set by when it
    publishes and by its API's limits."""

    schedule: str
    key_format: str
    timezone: str

    def snapshot_at(self, moment: datetime) -> str:
        calendar = dg.TimeWindowPartitionsDefinition(
            cron_schedule=self.schedule,
            fmt=self.key_format,
            start=CALENDAR_START,
            timezone=self.timezone,
            end_offset=1,
        )
        return calendar.get_partition_key_for_timestamp(moment.timestamp())


@dataclass(frozen=True, slots=True)
class ArchivedSource:
    """A source that goes through the raw archive, as the generic layer sees it: its name (its
    archive folder, and its dbt models' folders), when it is fetched, and in the live lake the
    fetch itself, which saves a snapshot's files to the archive and returns them by name.
    Everything Dagster does with a source is built from this, so adding one is an
    `ArchivedSource`, its fetch and its dbt models."""

    name: str
    cadence: SnapshotCadence
    fetch: Callable[[str], Mapping[str, Download]] | None = None


def snapshot_partitions(source: str, lake: PipelineMode) -> dg.DynamicPartitionsDefinition:
    """A source's partitions are its complete snapshots in the lake's archive, which the archive
    index adds as they appear, so no partition exists that has nothing to build from. Each source
    has its own, as each is fetched on its own cadence. Dynamic partitions are named
    instance-wide, so the name says the lake."""
    return dg.DynamicPartitionsDefinition(name=f"{lake.value}_{source}_snapshots")
