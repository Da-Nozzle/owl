from functools import partial

from owl_config.configuration import Configuration
from owl_dagster_orchestration.snapshots import ArchivedSource, SnapshotCadence
from owl_dagster_orchestration.sources.adapters import LiveAdapters
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.uzt_vacancies.files import (
    SNAPSHOT_KEY_FORMAT,
    SOURCE_NAME,
    archive_vacancies,
)
from owl_ingest.sources.uzt_vacancies.settings import load_uzt_vacancies_settings

# The Employment Service's datasets are drawn in one box, with the staging and intermediate
# models in models/staging/uzt/ and models/intermediate/uzt/.
UZT_GROUP = "uzt"


def uzt_vacancies_source(
    config: Configuration, timezone: str, archive: RawArchive, live: LiveAdapters | None
) -> ArchivedSource:
    settings = load_uzt_vacancies_settings(config)
    fetch = None
    if live is not None:
        fetch = partial(
            archive_vacancies,
            archive=archive,
            settings=settings,
            downloader=live.downloader("uzt_vacancies.http"),
            streaming=live.streaming_downloader("uzt_vacancies.http"),
        )
    return ArchivedSource(
        name=SOURCE_NAME,
        cadence=SnapshotCadence(
            schedule=settings.snapshot_schedule, key_format=SNAPSHOT_KEY_FORMAT, timezone=timezone
        ),
        fetch=fetch,
        fetch_max_runtime_seconds=settings.fetch_max_runtime_seconds,
        group=f"{UZT_GROUP}/vacancies",
    )
