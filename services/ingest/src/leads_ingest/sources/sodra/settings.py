from dataclasses import dataclass

from leads_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class SodraSettings:
    download_url_template: str
    history_months: int


def load_sodra_settings(config: Configuration) -> SodraSettings:
    history_months = config.get_int("sodra.history_months")
    if history_months < 1:
        raise ValueError(f"'sodra.history_months' is {history_months}, expected at least 1")
    return SodraSettings(
        download_url_template=config.get_str("sodra.download_url_template"),
        history_months=history_months,
    )
