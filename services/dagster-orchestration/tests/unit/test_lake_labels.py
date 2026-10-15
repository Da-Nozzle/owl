import dagster as dg

from leads_dagster_orchestration.lake_labels import lake_labels, with_labels
from leads_ingest.settings import PipelineMode
from leads_lake.settings import load_lake_settings
from leads_testing.config import build_config


def test_labels_name_the_mode_and_its_lake() -> None:
    # Arrange
    replay_lake = load_lake_settings(
        build_config({"lake.replay.metadata_schema": "ducklake_replay"}), replay=True
    )

    # Act
    labels = lake_labels(PipelineMode.REPLAY, replay_lake)

    # Assert
    assert labels == {"leads/mode": "replay", "leads/lake": "ducklake_replay"}


def test_a_materialization_carries_the_labels_beside_its_metadata() -> None:
    # Arrange
    event = dg.AssetMaterialization("raw", metadata={"dagster/row_count": 3})

    # Act
    labelled = with_labels(event, {"leads/mode": "live"})

    # Assert
    assert labelled.metadata["leads/mode"] == dg.MetadataValue.text("live")
    assert labelled.metadata["dagster/row_count"] == dg.MetadataValue.int(3)


def test_events_that_are_not_materializations_are_left_alone() -> None:
    # Arrange
    check = dg.AssetCheckResult(passed=True, asset_key="raw", check_name="c")

    # Act / Assert
    assert with_labels(check, {"leads/mode": "live"}) is check
