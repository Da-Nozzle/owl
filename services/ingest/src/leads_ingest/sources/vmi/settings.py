from dataclasses import dataclass

from leads_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class VmiSettings:
    # The taxpayer register's model on get.data.gov.lt: its CSV export and changelog are below it.
    register_url: str
    # The EVRK 2 classifier's model, which the register's activities refer to by _id.
    activities_url: str
    # When the day's changes are fetched (cron, in pipeline.timezone); a snapshot holds the
    # changes made in the day before the tick it is keyed by.
    daily_changes_schedule: str
    # The first day whose changes can be fetched: the changelog holds every past day, so the
    # changes are partitioned by day from here, and any of those days can be fetched again.
    first_daily_changes: str
    # When the whole register is fetched (cron, in pipeline.timezone): the baseline the changes
    # after it are applied to, keyed by the month of the tick it was fetched after.
    monthly_register_schedule: str


def load_vmi_settings(config: Configuration) -> VmiSettings:
    return VmiSettings(
        register_url=config.get_str("vmi.register_url"),
        activities_url=config.get_str("vmi.activities_url"),
        daily_changes_schedule=config.get_str("vmi.daily_changes_schedule"),
        first_daily_changes=config.get_str("vmi.first_daily_changes"),
        monthly_register_schedule=config.get_str("vmi.monthly_register_schedule"),
    )
