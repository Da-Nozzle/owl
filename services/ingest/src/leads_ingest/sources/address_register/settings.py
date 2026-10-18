from dataclasses import dataclass

from leads_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class AddressRegisterSettings:
    # A dataset's CSV export, by its {dataset} path.
    download_url_template: str
    snapshot_schedule: str


def load_address_register_settings(config: Configuration) -> AddressRegisterSettings:
    return AddressRegisterSettings(
        download_url_template=config.get_str("address_register.download_url_template"),
        snapshot_schedule=config.get_str("address_register.snapshot_schedule"),
    )
