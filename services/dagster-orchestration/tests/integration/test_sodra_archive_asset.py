from collections.abc import Mapping
from dataclasses import dataclass, field

import dagster as dg

from leads_dagster_orchestration.asset_keys import archive_key
from leads_dagster_orchestration.assets.sodra import build_sodra_archive_asset, files_metadata
from leads_dagster_orchestration.partitions import run_month_partitions
from leads_ingest.months import Month
from leads_ingest.ports.downloader import Download
from leads_ingest.settings import load_pipeline_settings
from leads_ingest.sources.sodra.files import SOURCE_NAME
from leads_testing.config import build_config
from leads_testing.ingest import build_download

PARTITIONS = run_month_partitions(
    load_pipeline_settings(build_config({"pipeline.first_month": "2026-05"}))
)
LABELS = {"leads/mode": "replay", "leads/lake": "ducklake_replay"}
FILES = {"monthly-2026.csv.zip": build_download(url="https://sodra.test/2026")}


@dataclass
class ArchiveFiles:
    """Stands in for archiving a run month's files, and records the run months asked for."""

    files: Mapping[str, Download]
    run_months: list[Month] = field(default_factory=list)

    def __call__(self, run_month: Month) -> Mapping[str, Download]:
        self.run_months.append(run_month)
        return self.files


def materialize(
    archive_files: ArchiveFiles, run_month: str, *, downloads: bool
) -> tuple[dg.ExecuteInProcessResult, dg.DagsterInstance]:
    instance = dg.DagsterInstance.ephemeral()
    asset = build_sodra_archive_asset(PARTITIONS, archive_files, LABELS, downloads=downloads)
    result = dg.materialize(
        [asset], partition_key=run_month, instance=instance, raise_on_error=False
    )
    return result, instance


def test_a_run_records_each_archived_files_fetch_and_the_runs_labels() -> None:
    # Arrange
    archive_files = ArchiveFiles(FILES)

    # Act
    result, instance = materialize(archive_files, "2026-06", downloads=False)

    # Assert
    assert result.success
    assert archive_files.run_months == [Month(2026, 6)]
    event = instance.get_latest_materialization_event(archive_key(SOURCE_NAME))
    assert event is not None
    assert event.asset_materialization is not None
    metadata = event.asset_materialization.metadata
    assert metadata["files"] == dg.MetadataValue.json(files_metadata(FILES))
    assert metadata["leads/mode"] == dg.MetadataValue.text("replay")
    run = instance.get_run_by_id(result.run_id)
    assert run is not None
    assert LABELS.items() <= run.tags.items()


def test_a_live_run_of_a_passed_run_month_archives_nothing() -> None:
    # Arrange
    archive_files = ArchiveFiles(FILES)

    # Act
    result, _ = materialize(archive_files, PARTITIONS.get_first_partition_key(), downloads=True)

    # Assert
    assert not result.success
    assert archive_files.run_months == []


def test_a_live_run_of_the_current_run_month_archives_its_files() -> None:
    # Arrange
    archive_files = ArchiveFiles(FILES)
    current = PARTITIONS.get_last_partition_key()
    assert current is not None

    # Act
    result, _ = materialize(archive_files, current, downloads=True)

    # Assert
    assert result.success
    assert archive_files.run_months == [Month.parse(current)]
