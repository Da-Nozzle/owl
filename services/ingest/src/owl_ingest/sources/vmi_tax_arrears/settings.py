from dataclasses import dataclass

from owl_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class VmiTaxArrearsSettings:
    # The Spinta model of the arrears list: its changelog is below it.
    model_url: str
    snapshot_schedule: str
    # The first snapshot partitioned, whose window holds the changelog's first change.
    first_snapshot: str
    changelog_page_size: int


def load_vmi_tax_arrears_settings(config: Configuration) -> VmiTaxArrearsSettings:
    page_size = config.get_int("vmi_tax_arrears.changelog_page_size")
    if page_size < 1:
        raise ValueError(f"'vmi_tax_arrears.changelog_page_size' must be at least 1: {page_size}")
    return VmiTaxArrearsSettings(
        model_url=config.get_str("vmi_tax_arrears.model_url"),
        snapshot_schedule=config.get_str("vmi_tax_arrears.snapshot_schedule"),
        first_snapshot=config.get_str("vmi_tax_arrears.first_snapshot"),
        changelog_page_size=page_size,
    )
