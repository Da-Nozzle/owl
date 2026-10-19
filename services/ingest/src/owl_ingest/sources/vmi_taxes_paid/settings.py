from dataclasses import dataclass

from owl_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class VmiTaxesPaidSettings:
    # The dataset's Spinta model URL: its changelog is below it.
    model_url: str
    snapshot_schedule: str
    first_snapshot: str
    changelog_page_size: int


def load_vmi_taxes_paid_settings(config: Configuration) -> VmiTaxesPaidSettings:
    page_size = config.get_int("vmi_taxes_paid.changelog_page_size")
    if page_size < 1:
        raise ValueError(f"'vmi_taxes_paid.changelog_page_size' must be at least 1: {page_size}")
    return VmiTaxesPaidSettings(
        model_url=config.get_str("vmi_taxes_paid.model_url"),
        snapshot_schedule=config.get_str("vmi_taxes_paid.snapshot_schedule"),
        first_snapshot=config.get_str("vmi_taxes_paid.first_snapshot"),
        changelog_page_size=page_size,
    )
