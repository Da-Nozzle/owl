from dataclasses import dataclass

from leads_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class AddressRegisterSettings:
    # A dataset's whole CSV export; {dataset} is its path below the register's, such as
    # pastatas/Pastatas.
    download_url_template: str
    # When a snapshot is fetched (cron, in pipeline.timezone); the snapshot is keyed by the
    # month of the tick it was fetched after.
    snapshot_schedule: str


def load_address_register_settings(config: Configuration) -> AddressRegisterSettings:
    return AddressRegisterSettings(
        download_url_template=config.get_str("address_register.download_url_template"),
        snapshot_schedule=config.get_str("address_register.snapshot_schedule"),
    )
