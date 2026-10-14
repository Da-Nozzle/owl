import dagster as dg
import pytest
from dagster_dlt.translator import DltResourceTranslatorData

from leads_dagster_orchestration.asset_keys import dbt_resource_key
from leads_dagster_orchestration.translators import RawTableDltTranslator
from leads_ingest.adapters.lake_probe.source import lake_probe_source


def test_dbt_source_gets_the_key_of_the_dlt_table_it_reads() -> None:
    # Arrange
    probe_rows = lake_probe_source().resources["probe_rows"]
    dbt_source_props = {
        "resource_type": "source",
        "source_name": "lake_probe",
        "name": "probe_rows",
    }

    # Act
    dlt_key = (
        RawTableDltTranslator()
        .get_asset_spec(DltResourceTranslatorData(resource=probe_rows, pipeline=None))
        .key
    )
    dbt_key = dbt_resource_key(dbt_source_props)

    # Assert
    assert dlt_key == dbt_key == dg.AssetKey(["raw", "lake_probe", "probe_rows"])


def test_dlt_table_has_no_upstream_assets() -> None:
    # Arrange
    probe_rows = lake_probe_source().resources["probe_rows"]

    # Act
    spec = RawTableDltTranslator().get_asset_spec(
        DltResourceTranslatorData(resource=probe_rows, pipeline=None)
    )

    # Assert
    assert list(spec.deps) == []


def test_dbt_model_is_keyed_by_its_lake_schema() -> None:
    # Arrange
    model_props = {
        "resource_type": "model",
        "name": "stg_lake_probe__probe_rows",
        "config": {"schema": "staging"},
    }

    # Act
    key = dbt_resource_key(model_props)

    # Assert
    assert key == dg.AssetKey(["staging", "stg_lake_probe__probe_rows"])


def test_dbt_model_without_a_schema_is_rejected() -> None:
    # Arrange
    model_props = {"resource_type": "model", "name": "orphan", "config": {"schema": None}}

    # Act / Assert
    with pytest.raises(ValueError, match="orphan"):
        dbt_resource_key(model_props)
