import dagster as dg
import pytest

from leads_dagster_orchestration.lake_labels import Trigger, lake_labels, run_trigger, with_labels
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


@pytest.mark.parametrize("fetches", [True, False])
def test_a_scheduled_run_is_labelled_schedule(fetches: bool) -> None:
    # Act
    trigger = run_trigger({"dagster/schedule_name": "fetch_sodra_schedule"}, fetches=fetches)

    # Assert
    assert trigger is Trigger.SCHEDULE


def test_a_run_dagster_started_for_a_change_is_labelled_automation() -> None:
    # Act
    trigger = run_trigger({"dagster/from_automation_condition": "true"}, fetches=False)

    # Assert
    assert trigger is Trigger.AUTOMATION


def test_a_run_by_hand_is_a_fetch_or_a_rebuild() -> None:
    # Arrange
    tags = {"dagster/backfill": "abc", "leads/mode": "live"}

    # Act / Assert
    assert run_trigger(tags, fetches=True) is Trigger.FETCH
    assert run_trigger(tags, fetches=False) is Trigger.REBUILD
