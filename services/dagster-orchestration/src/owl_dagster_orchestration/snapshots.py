from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

import dagster as dg

from owl_ingest.ports.downloader import Download, FetchedFile
from owl_ingest.settings import PipelineMode

# Any time before every source's first snapshot.
CALENDAR_START = datetime(2000, 1, 1)


@dataclass(frozen=True, slots=True)
class SnapshotCadence:
    """A snapshot is the period from one tick of `schedule` (cron, in `timezone`) to the next,
    keyed with `key_format`; a second fetch in a period replaces its snapshot."""

    schedule: str
    key_format: str
    timezone: str

    def snapshot_at(self, moment: datetime) -> str:
        return self.periods(CALENDAR_START).get_partition_key_for_timestamp(moment.timestamp())

    def window_before(self, snapshot: str) -> tuple[datetime, datetime]:
        """From the tick before the snapshot's up to its own: a changelog's snapshot holds the
        changes made then, all published by its tick."""
        periods = self.periods(CALENDAR_START)
        end = periods.time_window_for_partition_key(snapshot).start
        start = periods.time_window_for_partition_key(
            periods.get_partition_key_for_timestamp(end.timestamp() - 1)
        ).start
        return start, end

    def periods(self, start: datetime | str) -> dg.TimeWindowPartitionsDefinition:
        return dg.TimeWindowPartitionsDefinition(
            cron_schedule=self.schedule,
            fmt=self.key_format,
            start=start,
            timezone=self.timezone,
            end_offset=1,
        )


@dataclass(frozen=True, slots=True)
class FetchPlan:
    """What a paid source's fetch will pay for, found without paying."""

    paid_requests: int
    max_paid_requests: int
    summary: str
    # One row per part (Google's search areas), as columns by name.
    parts: Sequence[Mapping[str, object]]
    # Pays for exactly this plan, archiving it as the snapshot given.
    pay: Callable[[str], Mapping[str, Download]]

    @property
    def within_budget(self) -> bool:
        return self.paid_requests <= self.max_paid_requests


@dataclass(frozen=True, slots=True)
class ArchivedSource:
    """Everything Dagster builds for a source comes from this; its name is its archive folder
    and its dbt models' folder. Live only: `fetch`, or for a source billed per request, `plan`."""

    name: str
    cadence: SnapshotCadence
    # Archives a snapshot's files and returns them by name.
    fetch: Callable[[str], Mapping[str, FetchedFile]] | None = None
    # For a source that keeps its history, as a changelog does: any period from here can be
    # fetched. Otherwise only the current period can.
    history_start: str | None = None
    # A path within the lake's group, for sources of one dataset drawn together (vmi/...).
    group: str | None = None
    # Overrides the instance's max_runtime_seconds for the fetch and the plan's runs.
    fetch_max_runtime_seconds: int | None = None
    # False for a paid source, so that its first fetch is started on purpose, not by a deploy.
    schedule_running: bool = True
    # Lake assets the fetch or plan reads besides the source.
    fetch_deps: Sequence[dg.AssetKey] = ()
    plan: Callable[[], FetchPlan] | None = None

    def __post_init__(self) -> None:
        if self.fetch is not None and self.plan is not None:
            raise ValueError(f"Source {self.name!r} has a fetch and a plan; a plan is its fetch")

    @property
    def group_path(self) -> str:
        return self.group or self.name

    @property
    def fetches(self) -> bool:
        return self.fetch is not None or self.plan is not None

    def fetch_periods(self) -> dg.TimeWindowPartitionsDefinition | None:
        if self.history_start is None:
            return None
        start = datetime.strptime(self.history_start, self.cadence.key_format)
        return self.cadence.periods(start)


def snapshot_partitions(source: str, lake: PipelineMode) -> dg.DynamicPartitionsDefinition:
    """Added by the archive index as snapshots complete. Named instance-wide, hence the lake."""
    return dg.DynamicPartitionsDefinition(name=f"{lake.value}_{source}_snapshots")
