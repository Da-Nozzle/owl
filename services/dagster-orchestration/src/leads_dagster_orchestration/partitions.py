import dagster as dg

from leads_ingest.months import MONTHS_PER_YEAR, Month
from leads_ingest.settings import PipelineSettings

PARTITION_FORMAT = "%Y-%m"


def run_month_partitions(settings: PipelineSettings) -> dg.MonthlyPartitionsDefinition:
    """One partition per run month, keyed YYYY-MM. Run month M starts on `run_day` of M at
    `run_hour` local time, when its sources have published, and its partition exists from then;
    the current month is a partition too."""
    return dg.MonthlyPartitionsDefinition(
        start_date=settings.first_month.key,
        fmt=PARTITION_FORMAT,
        day_offset=settings.run_day,
        hour_offset=settings.run_hour,
        timezone=settings.timezone,
        end_offset=1,
    )


def replay_month_partitions(
    months: list[Month], settings: PipelineSettings
) -> dg.MonthlyPartitionsDefinition:
    """One partition per run month the replay sample holds, keyed YYYY-MM like live runs. The
    sample is already published, so its months need no run day and exist from their first day."""
    if not months:
        raise ValueError(f"The replay sample in {settings.replay_path} holds no run month")
    # A monthly partitions definition is a range, so a gap would be a partition with no files.
    first, last = min(months), max(months)
    span = (last.year - first.year) * MONTHS_PER_YEAR + last.month - first.month
    missing = [first.plus(n).key for n in range(span + 1) if first.plus(n) not in months]
    if missing:
        raise ValueError(f"The replay sample in {settings.replay_path} lacks run months {missing}")
    return dg.MonthlyPartitionsDefinition(
        start_date=first.key,
        end_date=last.plus(1).key,
        fmt=PARTITION_FORMAT,
        timezone=settings.timezone,
        end_offset=1,
    )
