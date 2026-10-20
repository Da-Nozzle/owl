import io
import subprocess
import zipfile
from collections.abc import Mapping, MutableMapping
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from owl_config.configuration import Configuration
from owl_dagster_orchestration.dbt.environment import SNAPSHOT_VARIABLE, dbt_environment
from owl_dagster_orchestration.dbt.resource import dbt_core_executable
from owl_dagster_orchestration.sources.adapters import build_raw_archive
from owl_ingest.adapters.ducklake.search_areas import (
    LAKE,
    SEARCH_AREAS_MODEL,
    SEARCH_AREAS_SCHEMA,
    DuckLakeSearchAreaReader,
    attach_statements,
)
from owl_ingest.ports.search_areas import BoundingBox, SearchArea, SearchMode
from owl_ingest.settings import PipelineMode, load_pipeline_settings
from owl_ingest.sources.sodra.files import SOURCE_NAME, archive_yearly_files
from owl_ingest.sources.sodra.settings import load_sodra_settings
from owl_lake.settings import load_lake_settings, load_lake_storage
from owl_testing.ingest import FakeDownloader

SNAPSHOT = "2026-09"
COMPLETED_AT = datetime(2026, 10, 21, 3, 5, tzinfo=UTC)
MEMORY_LIMIT = "1GB"
# Sodra's yearly file, as it publishes it: one CSV in a zip, its headers as sent.
SODRA_HEADER = (
    "Draudėjo kodas (code)",
    "Juridinių asmenų registro kodas (jarCode)",
    "Pavadinimas (name)",
    "Savivaldybė, kurioje registruota(municipality)",
    "Ekonominės veiklos rūšies kodas(ecoActCode)",
    "Ekonominės veiklos rūšies pavadinimas(ecoActName)",
    "Mėnuo (month)",
    "Vidutinis darbo užmokestis (avgWage)",
    "Apdraustųjų skaičius (numInsured)",
    "Valstybinio socialinio draudimo įmoka (tax)",
)
# In August, Vilnius has a café, a hair salon and a shop (in none of the leads' categories), and
# Kaunas a café. The café's July is before the latest month, the only one the areas count.
SODRA_ROWS = (
    ("41055", "110379432", "UAB Kavinė", "Vilniaus m. sav.", "561100", "Restoranai", "202607"),
    ("41055", "110379432", "UAB Kavinė", "Vilniaus m. sav.", "561100", "Restoranai", "202608"),
    ("41056", "110379433", "UAB Kirpykla", "Vilniaus m. sav.", "962100", "Kirpyklos", "202608"),
    ("41057", "110379434", "UAB Parduotuvė", "Vilniaus m. sav.", "471100", "Prekyba", "202608"),
    ("41058", "110379435", "UAB Kauno kavinė", "Kauno m. sav.", "561100", "Restoranai", "202608"),
)


def sodra_file() -> bytes:
    # Each employer's average wage, insured and contributions alike.
    rows = [(*row, "1284.54", "5", "1620.84") for row in SODRA_ROWS]
    lines = [";".join(SODRA_HEADER), *(";".join(f'"{value}"' for value in row) for row in rows)]
    body = io.BytesIO()
    with zipfile.ZipFile(body, "w") as zipped:
        zipped.writestr("monthly-2026.csv", "\n".join(lines) + "\n")
    return body.getvalue()


def archive_sodra_snapshot(config: Configuration) -> None:
    """As live's fetch of Sodra keeps it: in the archive on S3, its year's file only."""
    archive = build_raw_archive(
        PipelineMode.LIVE, load_pipeline_settings(config), load_lake_storage(config)
    )
    settings = replace(load_sodra_settings(config), history_months=1)
    downloads = archive_yearly_files(SNAPSHOT, archive, settings, FakeDownloader(sodra_file()))
    archive.complete(SOURCE_NAME, SNAPSHOT, downloads, COMPLETED_AT)


def dbt_build(
    selection: str,
    config: Configuration,
    project: Path,
    tmp_path: Path,
    environment: Mapping[str, str],
) -> None:
    """`dbt build` into the lake with the project's own profile, as Dagster's run of the snapshot
    builds it."""
    variables = {
        **environment,
        **dbt_environment(config, PipelineMode.LIVE),
        SNAPSHOT_VARIABLE: SNAPSHOT,
        # Not in the project's target/, which the dev tools use.
        "OWL_DUCKDB_TEMP_DIRECTORY": str(tmp_path / "duckdb_tmp"),
    }
    result = subprocess.run(
        [
            dbt_core_executable(),
            "build",
            "--select",
            selection,
            # Not the tests that also read a model outside it.
            "--indirect-selection",
            "cautious",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--target-path",
            str(tmp_path / "target"),
            "--log-path",
            str(tmp_path / "logs"),
        ],
        env=variables,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout


def test_a_snapshot_archived_on_s3_builds_into_the_lake_where_googles_plan_reads_it(
    services_config: Configuration,
    pytestconfig: pytest.Config,
    tmp_path: Path,
    environ: MutableMapping[str, str],
) -> None:
    # Arrange
    archive_sodra_snapshot(services_config)
    project = pytestconfig.rootpath / "services" / "transform"

    # Act
    dbt_build(f"+{SEARCH_AREAS_MODEL}", services_config, project, tmp_path, environ)

    # Assert: read back by a DuckDB of its own, as Google's plan reads it
    settings = load_lake_settings(services_config)
    areas = DuckLakeSearchAreaReader(settings, MEMORY_LIMIT).search_areas()
    assert areas == [
        SearchArea(
            13,
            "Vilniaus miesto sav.",
            SearchMode.FETCH,
            BoundingBox(54.5689, 25.0245, 54.8324, 25.4815),
        )
    ]
    with duckdb.connect() as connection:
        for statement in attach_statements(settings, MEMORY_LIMIT):
            connection.execute(statement)
        employers = connection.execute(
            "SELECT municipality_code, target_employers, declared_month "
            f"FROM {LAKE}.{SEARCH_AREAS_SCHEMA}.{SEARCH_AREAS_MODEL} "
            "WHERE target_employers > 0 ORDER BY municipality_code"
        ).fetchall()
    assert employers == [(13, 2, date(2026, 8, 1)), (19, 1, date(2026, 8, 1))]
