import dagster as dg

from leads_dagster_orchestration.lake_labels import lake_labels, with_labels
from leads_ingest.settings import PipelineMode
from leads_lake.settings import LakeCatalog, LakeSettings, LakeStorage

REPLAY_LAKE = LakeSettings(
    catalog=LakeCatalog("postgres", 5432, "ducklake_catalog", "leads", "leads", "ducklake_replay"),
    storage=LakeStorage(
        "s3://leads-lake/replay/",
        "s3://leads-lake/archive/",
        "s3mock:9090",
        "eu-central-1",
        "path",
        False,
        "s3mock",
        "s3mock",
    ),
)


def test_labels_name_the_mode_and_its_lake() -> None:
    # Act
    labels = lake_labels(PipelineMode.REPLAY, REPLAY_LAKE)

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
