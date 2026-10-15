from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtProject
from dagster_dlt import DagsterDltTranslator
from dagster_dlt.translator import DltResourceTranslatorData

from leads_dagster_orchestration.asset_keys import dbt_resource_key, layer_group, raw_table_key


class RawTableDltTranslator(DagsterDltTranslator):
    """Keys each dlt resource as raw/raw_<source>__<table>, with no upstream assets."""

    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        table = data.resource.table_name
        if not isinstance(table, str):
            raise TypeError(f"dlt resource {data.resource.name!r} has a dynamic table name")
        key = raw_table_key(data.resource.source_name, table)
        return (
            super()
            .get_asset_spec(data)
            .replace_attributes(key=key, group_name=layer_group(key), deps=[])
        )


class LakeDbtTranslator(DagsterDbtTranslator):
    """Keys dbt resources so that dbt, dlt and the archive form one graph, grouped by layer."""

    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: DbtProject | None
    ) -> dg.AssetSpec:
        key = dbt_resource_key(self.get_resource_props(manifest, unique_id))
        return (
            super()
            .get_asset_spec(manifest, unique_id, project)
            .replace_attributes(key=key, group_name=layer_group(key))
        )
