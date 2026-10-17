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
        return self.periods(CALENDAR_START).get_partition_key_for_timestamp(moment.timestamp())

    def window_before(self, snapshot: str) -> tuple[datetime, datetime]:
        """The period before the snapshot's, from its tick up to the snapshot's own: what a
        source published in it is what was new when the snapshot's period began. A changelog's
        snapshot holds the changes made in it, so a fetch at the snapshot's tick gets all of
        them."""
        periods = self.periods(CALENDAR_START)
        end = periods.time_window_for_partition_key(snapshot).start
        start = periods.time_window_for_partition_key(
            periods.get_partition_key_for_timestamp(end.timestamp() - 1)
        ).start
        return start, end

    def periods(self, start: datetime | str) -> dg.TimeWindowPartitionsDefinition:
        """The periods from `start` up to the current one, each keyed as its snapshot."""
        return dg.TimeWindowPartitionsDefinition(
            cron_schedule=self.schedule,
            fmt=self.key_format,
            start=start,
            timezone=self.timezone,
            end_offset=1,
        )


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
    # The first snapshot of a source that keeps its history, as a changelog does: any period
    # from there on can be fetched, whenever. None for a source that can only be captured as it
    # is now, whose fetch is the current period's.
    history_start: str | None = None
    # Where in its lake's group the source's assets are drawn: its name, unless it is one of
    # several sources of one dataset, drawn together in a group of the dataset's (vmi/...).
    group: str | None = None

    @property
    def group_path(self) -> str:
        return self.group or self.name

    def fetch_periods(self) -> dg.TimeWindowPartitionsDefinition | None:
        """The periods a source with history can be fetched for, up to the current one."""
        if self.history_start is None:
            return None
        start = datetime.strptime(self.history_start, self.cadence.key_format)
        return self.cadence.periods(start)


def snapshot_partitions(source: str, lake: PipelineMode) -> dg.DynamicPartitionsDefinition:
    """A source's partitions are its complete snapshots in the lake's archive, which the archive
    index adds as they appear, so no partition exists that has nothing to build from. Each source
    has its own, as each is fetched on its own cadence. Dynamic partitions are named
    instance-wide, so the name says the lake."""
    return dg.DynamicPartitionsDefinition(name=f"{lake.value}_{source}_snapshots")
