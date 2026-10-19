import json
import subprocess
from collections.abc import Callable, MutableMapping
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from owl_dagster_orchestration.dbt.environment import risk_model_environment
from owl_dagster_orchestration.dbt.resource import dbt_core_executable
from owl_ingest.adapters.archive.fsspec_archive import FsspecRawArchive, open_fsspec_archive
from owl_ingest.ports.downloader import OpenedDownload, StreamedDownload
from owl_testing.ingest import build_download, change_entry

COMPLETED_AT = datetime(2026, 10, 21, 3, 5, tzinfo=UTC)
# A DuckDB file and nothing attached: the raw models read only the archive.
PROFILE = """owl:
  target: test
  outputs:
    test:
      type: duckdb
      path: "{path}"
      threads: 1
"""


@pytest.fixture
def archive(tmp_path: Path) -> FsspecRawArchive:
    return open_fsspec_archive(str(tmp_path / "archive"))


@pytest.fixture
def build(
    pytestconfig: pytest.Config, tmp_path: Path, environ: MutableMapping[str, str]
) -> Callable[[str], list[tuple[str, ...]]]:
    """Builds a raw model from nothing with dbt over the archive in `tmp_path`, and returns its
    rows' files (the archive's own path left out) and ids. dbt runs in a process of its own, as
    dbt-duckdb keeps its database open in the process that ran it."""
    project = pytestconfig.rootpath / "services" / "transform"
    (tmp_path / "profiles.yml").write_text(PROFILE.format(path=tmp_path / "raw.duckdb"))
    environ["OWL_ARCHIVE_PATH"] = str(tmp_path / "archive")
    # Read while dbt parses, as every launcher sets it.
    environ.update(risk_model_environment(project))

    def built_rows(model: str) -> list[tuple[str, ...]]:
        result = subprocess.run(
            [
                dbt_core_executable(),
                "run",
                "--select",
                model,
                "--project-dir",
                str(project),
                "--profiles-dir",
                str(tmp_path),
                "--target-path",
                str(tmp_path / "target"),
                "--log-path",
                str(tmp_path / "logs"),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout
        with duckdb.connect(str(tmp_path / "raw.duckdb"), read_only=True) as connection:
            schema, table = connection.sql(
                "select table_schema, table_name from information_schema.tables"
            ).fetchall()[0]
            return connection.sql(
                f"select replace(_source_file, '{tmp_path}/archive/', ''), _id "
                f'from "{schema}"."{table}" order by all'
            ).fetchall()

    return built_rows


def vacancy_page(
    name: str, rows: list[tuple[str, str]], archive: FsspecRawArchive
) -> StreamedDownload:
    """A page of the vacancies' export, saved as the paged fetch saves it (gzipped)."""
    body = "_id,darbo_vietos_id\n" + "".join(f"{row},{vacancy}\n" for row, vacancy in rows)
    recorded = build_download(url="https://get.data.gov.lt/datasets/gov/uzt/ldv/Vieta/:format/csv")
    return archive.save_stream(
        "uzt_vacancies",
        "2026-09",
        name,
        OpenedDownload(
            url=recorded.url,
            fetched_at=recorded.fetched_at,
            last_modified=recorded.last_modified,
            etag=recorded.etag,
            chunks=iter([body.encode()]),
        ),
    )


def test_a_csv_page_left_by_an_earlier_fetch_of_the_period_is_not_read(
    archive: FsspecRawArchive, build: Callable[[str], list[tuple[str, ...]]], tmp_path: Path
) -> None:
    # Arrange
    earlier = {
        "Vieta.csv.gz": vacancy_page("Vieta.csv.gz", [("v1", "DV-1"), ("v2", "DV-2")], archive),
        "Vieta.2.csv.gz": vacancy_page("Vieta.2.csv.gz", [("v3", "DV-3"), ("v4", "DV-4")], archive),
        "Vieta.3.csv.gz": vacancy_page("Vieta.3.csv.gz", [("v5", "DV-5")], archive),
    }
    archive.complete("uzt_vacancies", "2026-09", earlier, COMPLETED_AT)
    # Fetched again in the period, after the dataset was reloaded with fewer rows: two pages.
    again = {
        "Vieta.csv.gz": vacancy_page("Vieta.csv.gz", [("w1", "DV-1"), ("w2", "DV-2")], archive),
        "Vieta.2.csv.gz": vacancy_page("Vieta.2.csv.gz", [("w3", "DV-3")], archive),
    }
    archive.complete("uzt_vacancies", "2026-09", again, COMPLETED_AT)

    # Act
    rows = build("raw_uzt_vacancies__vacancies")

    # Assert
    assert (tmp_path / "archive" / "uzt_vacancies" / "2026-09" / "Vieta.3.csv.gz").exists()
    assert rows == [
        ("uzt_vacancies/2026-09/Vieta.2.csv.gz", "w3"),
        ("uzt_vacancies/2026-09/Vieta.csv.gz", "w1"),
        ("uzt_vacancies/2026-09/Vieta.csv.gz", "w2"),
    ]


def test_a_json_page_left_by_an_earlier_fetch_of_the_period_is_not_read(
    archive: FsspecRawArchive, build: Callable[[str], list[tuple[str, ...]]]
) -> None:
    # Arrange
    def changes_page(name: str, *change_ids: int) -> None:
        entries = [change_entry(change_id, "2026-09-11T03:00:00") for change_id in change_ids]
        archive.save(
            "vmi_daily_changes",
            "2026-09-11",
            name,
            build_download(json.dumps({"_data": entries}).encode()),
        )

    changes_page("changes-000000000001.json", 1, 2)
    changes_page("changes-000000000003.json", 3)
    # Fetched again, a page holds the day's changes the earlier fetch took two for.
    changes_page("changes-000000000001.json", 1, 2, 3)
    listed = {"changes-000000000001.json": build_download()}
    archive.complete("vmi_daily_changes", "2026-09-11", listed, COMPLETED_AT)

    # Act
    rows = build("raw_vmi_daily_changes__changes")

    # Assert
    page = "vmi_daily_changes/2026-09-11/changes-000000000001.json"
    assert rows == [(page, "row-1"), (page, "row-2"), (page, "row-3")]
