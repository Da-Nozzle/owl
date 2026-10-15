import dagster as dg
import dlt
import pytest
from dagster_dlt.translator import DltResourceTranslatorData

from leads_dagster_orchestration.asset_keys import archive_key, dbt_resource_key, layer_group
from leads_dagster_orchestration.translators import RawTableDltTranslator
from leads_ingest.sources.sodra.files import SOURCE_NAME


def test_dbt_archive_source_gets_the_key_of_the_archive_asset() -> None:
    # Arrange
    dbt_source_props = {"resource_type": "source", "source_name": "archive", "name": "sodra"}

    # Act
    key = dbt_resource_key(dbt_source_props)

    # Assert
    assert key == archive_key(SOURCE_NAME) == dg.AssetKey(["archive", "archive_sodra"])


def test_dbt_model_in_a_raw_schema_is_in_the_raw_layer() -> None:
    # Arrange
    model_props = {
        "resource_type": "model",
        "name": "raw_sodra__monthly",
        "alias": "monthly",
        "config": {"schema": "raw_sodra"},
    }

    # Act
    key = dbt_resource_key(model_props)

    # Assert
    assert key == dg.AssetKey(["raw", "raw_sodra__monthly"])


def test_dbt_source_of_a_dlt_table_gets_the_key_of_that_table() -> None:
    # Arrange
    @dlt.source(name="google")
    def google() -> object:
        return dlt.resource([{"id": 1}], name="listings")

    resource = google().resources["listings"]
    dbt_source_props = {"resource_type": "source", "source_name": "google", "name": "listings"}

    # Act
    dlt_key = (
        RawTableDltTranslator()
        .get_asset_spec(DltResourceTranslatorData(resource=resource, pipeline=None))
        .key
    )

    # Assert
    assert (
        dlt_key
        == dbt_resource_key(dbt_source_props)
        == dg.AssetKey(["raw", "raw_google__listings"])
    )


def test_dbt_model_is_keyed_by_its_lake_schema() -> None:
    # Arrange
    model_props = {
        "resource_type": "model",
        "name": "stg_sodra__monthly",
        "config": {"schema": "staging"},
    }

    # Act
    key = dbt_resource_key(model_props)

    # Assert
    assert key == dg.AssetKey(["staging", "stg_sodra__monthly"])


def test_dbt_model_without_a_schema_is_rejected() -> None:
    # Arrange
    model_props = {"resource_type": "model", "name": "orphan", "config": {"schema": None}}

    # Act / Assert
    with pytest.raises(ValueError, match="orphan"):
        dbt_resource_key(model_props)


def test_an_asset_is_grouped_by_its_layer() -> None:
    # Arrange
    key = dbt_resource_key({"resource_type": "source", "source_name": "archive", "name": "sodra"})

    # Act
    group = layer_group(key)

    # Assert
    assert group == "archive"
