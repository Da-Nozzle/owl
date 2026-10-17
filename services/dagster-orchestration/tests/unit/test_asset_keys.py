import dagster as dg
import dlt
import pytest
from dagster_dlt.translator import DltResourceTranslatorData

from leads_dagster_orchestration.asset_keys import (
    archive_key,
    dbt_resource_key,
    fetch_key,
    is_archive,
    lake_group,
    source_group,
)
from leads_dagster_orchestration.translators import RawTableDltTranslator
from leads_ingest.settings import PipelineMode
from leads_ingest.sources.sodra.files import SOURCE_NAME

LIVE = PipelineMode.LIVE
REPLAY = PipelineMode.REPLAY


def test_dbt_archive_source_gets_the_key_of_the_archive_asset() -> None:
    # Arrange
    dbt_source_props = {"resource_type": "source", "source_name": "archive", "name": "sodra"}

    # Act
    key = dbt_resource_key(dbt_source_props, LIVE)

    # Assert
    assert (
        key == archive_key(SOURCE_NAME, LIVE) == dg.AssetKey(["live", "archive", "archive_sodra"])
    )


def test_dbt_model_in_a_raw_schema_is_in_the_raw_layer() -> None:
    # Arrange
    model_props = {
        "resource_type": "model",
        "name": "raw_sodra__monthly",
        "alias": "monthly",
        "config": {"schema": "raw_sodra"},
    }

    # Act
    key = dbt_resource_key(model_props, LIVE)

    # Assert
    assert key == dg.AssetKey(["live", "raw", "raw_sodra__monthly"])


def test_dbt_source_of_a_dlt_table_gets_the_key_of_that_table() -> None:
    # Arrange
    @dlt.source(name="google")
    def google() -> object:
        return dlt.resource([{"id": 1}], name="listings")

    resource = google().resources["listings"]
    dbt_source_props = {"resource_type": "source", "source_name": "google", "name": "listings"}

    # Act
    dlt_spec = RawTableDltTranslator(LIVE).get_asset_spec(
        DltResourceTranslatorData(resource=resource, pipeline=None)
    )

    # Assert
    assert (
        dlt_spec.key
        == dbt_resource_key(dbt_source_props, LIVE)
        == dg.AssetKey(["live", "raw", "raw_google__listings"])
    )
    assert dlt_spec.group_name == "live/google"


def test_dbt_model_is_keyed_by_its_lake_and_lake_schema() -> None:
    # Arrange
    model_props = {
        "resource_type": "model",
        "name": "stg_sodra__monthly",
        "config": {"schema": "staging"},
    }

    # Act
    live = dbt_resource_key(model_props, LIVE)
    replay = dbt_resource_key(model_props, REPLAY)

    # Assert
    assert live == dg.AssetKey(["live", "staging", "stg_sodra__monthly"])
    assert replay == dg.AssetKey(["replay", "staging", "stg_sodra__monthly"])


def test_dbt_model_without_a_schema_is_rejected() -> None:
    # Arrange
    model_props = {"resource_type": "model", "name": "orphan", "config": {"schema": None}}

    # Act / Assert
    with pytest.raises(ValueError, match="orphan"):
        dbt_resource_key(model_props, LIVE)


def test_each_lake_is_a_group_of_its_own() -> None:
    # Act / Assert
    assert (lake_group(LIVE), lake_group(REPLAY)) == ("live", "replay")


def test_each_sources_group_is_nested_in_its_lakes() -> None:
    # Act / Assert
    assert source_group(SOURCE_NAME, LIVE) == "live/sodra"
    assert source_group(SOURCE_NAME, REPLAY) == "replay/sodra"


def test_an_archive_is_known_by_its_layer_in_either_lake() -> None:
    # Act / Assert
    assert is_archive(archive_key(SOURCE_NAME, LIVE))
    assert is_archive(archive_key(SOURCE_NAME, REPLAY))
    assert not is_archive(dg.AssetKey(["live", "raw", "raw_sodra__monthly"]))
    assert not is_archive(dg.AssetKey(["archive"]))


def test_a_sources_fetch_is_in_the_fetch_layer() -> None:
    # Act / Assert
    assert fetch_key(SOURCE_NAME, LIVE) == dg.AssetKey(["live", "fetch", "fetch_sodra"])
