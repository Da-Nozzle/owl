from pathlib import Path

import duckdb

from leads_dagster_orchestration.assets.transform import ensure_metadata_database, keep_main_state


def test_main_state_is_replaced_by_the_latest_builds_manifest(tmp_path: Path) -> None:
    # Arrange
    state_dir = tmp_path / "target" / "main"
    state_dir.mkdir(parents=True)
    (state_dir / "manifest.json").write_text('{"build": 1}')
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"build": 2}')

    # Act
    keep_main_state(manifest, state_dir)

    # Assert
    assert [path.name for path in state_dir.iterdir()] == ["manifest.json"]
    assert (state_dir / "manifest.json").read_text() == '{"build": 2}'


def test_main_state_is_kept_from_the_first_build(tmp_path: Path) -> None:
    # Arrange
    state_dir = tmp_path / "target" / "main"
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"build": 1}')

    # Act
    keep_main_state(manifest, state_dir)

    # Assert
    assert (state_dir / "manifest.json").read_text() == '{"build": 1}'


def test_metadata_database_opens_read_only(tmp_path: Path) -> None:
    # Arrange
    path = tmp_path / "target" / "dagster.duckdb"

    # Act
    ensure_metadata_database(path)
    ensure_metadata_database(path)

    # Assert
    with duckdb.connect(str(path), read_only=True) as connection:
        assert connection.execute("select 1").fetchone() == (1,)
    assert [entry.name for entry in path.parent.iterdir()] == ["dagster.duckdb"]
