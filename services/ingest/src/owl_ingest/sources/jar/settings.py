from dataclasses import dataclass

from owl_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class JarSettings:
    # A dataset's CSV export, by its {dataset} path.
    download_url_template: str
    snapshot_schedule: str


def load_jar_settings(config: Configuration) -> JarSettings:
    return JarSettings(
        download_url_template=config.get_str("jar.download_url_template"),
        snapshot_schedule=config.get_str("jar.snapshot_schedule"),
    )
