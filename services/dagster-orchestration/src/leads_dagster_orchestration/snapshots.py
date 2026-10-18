from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

import dagster as dg

from leads_ingest.ports.downloader import Download
from leads_ingest.settings import PipelineMode

# Any time before the first snapshot of any source: where a cadence's calendar starts counting.
CALENDAR_START = datetime(2000, 1, 1)
# The run tag run monitoring reads a run's longest runtime from, in seconds.
MAX_RUNTIME_TAG = "dagster/max_runtime"


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
class FetchPlan:
    """What a paid source's fetch will pay for, found without paying: its paid requests, the
    most a fetch may make, a sentence saying so (with money, as the source counts it), the parts
    it is made of, one row each (Google's search areas), as columns by name, and the payment
    itself: `pay` pays for exactly this plan, saving it to the archive as the snapshot given,
    and returns the files by name, failing before it pays for anything over the budget."""

    paid_requests: int
    max_paid_requests: int
    summary: str
    parts: Sequence[Mapping[str, object]]
    pay: Callable[[str], Mapping[str, Download]]

    @property
    def within_budget(self) -> bool:
        return self.paid_requests <= self.max_paid_requests


@dataclass(frozen=True, slots=True)
class ArchivedSource:
    """A source that goes through the raw archive, as the generic layer sees it: its name (its
    archive folder, and its dbt models' folders), when it is fetched, and in the live lake the
    fetch itself, which saves a snapshot's files to the archive and returns them by name; or,
    for a source billed per request, its plan, which a fetch pays for. Everything Dagster does
    with a source is built from this, so adding one is an `ArchivedSource`, its fetch (or plan)
    and its dbt models."""

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
    # How long its fetch job (and its plan's runs, which plan as the fetch does) may run before run
    # monitoring fails it, for a fetch that takes longer than the instance's
    # max_runtime_seconds; None keeps the instance's.
    fetch_max_runtime_seconds: int | None = None
    # Whether its fetch schedule runs from the start. A source billed per request starts with
    # it stopped, so that its first fetch is started on purpose, not by a deploy.
    schedule_running: bool = True
    # Assets of the lake its fetch reads besides the source, drawn upstream of its fetch, or of
    # its plan: what Google is searched for is a dbt model.
    fetch_deps: Sequence[dg.AssetKey] = ()
    # For a source billed per request, in place of `fetch`: plans its next fetch for free, which
    # is planned again whenever `fetch_deps` change, and which a fetch pays for.
    plan: Callable[[], FetchPlan] | None = None

    def __post_init__(self) -> None:
        if self.fetch is not None and self.plan is not None:
            raise ValueError(f"Source {self.name!r} has a fetch and a plan; a plan is its fetch")

    @property
    def group_path(self) -> str:
        return self.group or self.name

    @property
    def fetches(self) -> bool:
        """Whether the live lake fetches the source, by its fetch or by paying its plan."""
        return self.fetch is not None or self.plan is not None

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
