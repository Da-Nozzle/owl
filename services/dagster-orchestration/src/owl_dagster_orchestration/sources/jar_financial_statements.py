from functools import partial

from owl_config.configuration import Configuration
from owl_dagster_orchestration.snapshots import ArchivedSource, SnapshotCadence
from owl_dagster_orchestration.sources.adapters import LiveAdapters
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.jar_financial_statements.files import (
    SNAPSHOT_KEY_FORMAT,
    SOURCE_NAME,
    archive_statement_files,
)
from owl_ingest.sources.jar_financial_statements.settings import (
    load_jar_financial_statements_settings,
)


def jar_financial_statements_source(
    config: Configuration, timezone: str, archive: RawArchive, live: LiveAdapters | None
) -> ArchivedSource:
    settings = load_jar_financial_statements_settings(config)
    fetch = None
    if live is not None:
        fetch = partial(
            archive_statement_files,
            archive=archive,
            settings=settings,
            downloader=live.streaming_downloader("jar_financial_statements.http"),
        )
    return ArchivedSource(
        name=SOURCE_NAME,
        cadence=SnapshotCadence(
            schedule=settings.snapshot_schedule, key_format=SNAPSHOT_KEY_FORMAT, timezone=timezone
        ),
        fetch=fetch,
        fetch_max_runtime_seconds=settings.fetch_max_runtime_seconds,
    )
