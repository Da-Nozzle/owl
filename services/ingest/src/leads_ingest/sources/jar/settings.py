from dataclasses import dataclass

from leads_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class JarSettings:
    # A dataset's whole CSV export; {dataset} is its path below the register's, such as
    # iregistruoti/JuridinisAsmuo.
    download_url_template: str
    # When a snapshot is fetched (cron, in pipeline.timezone); the snapshot is keyed by the
    # month of the tick it was fetched after.
    snapshot_schedule: str


def load_jar_settings(config: Configuration) -> JarSettings:
    return JarSettings(
        download_url_template=config.get_str("jar.download_url_template"),
        snapshot_schedule=config.get_str("jar.snapshot_schedule"),
    )
