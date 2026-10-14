from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtProject
from dagster_dlt import DagsterDltTranslator
from dagster_dlt.translator import DltResourceTranslatorData

from leads_dagster_orchestration.asset_keys import dbt_resource_key, raw_table_key


class RawTableDltTranslator(DagsterDltTranslator):
    """Keys each dlt resource as raw/<source>/<table>, with no upstream assets."""

    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        table = data.resource.table_name
        if not isinstance(table, str):
            raise TypeError(f"dlt resource {data.resource.name!r} has a dynamic table name")
        return (
            super()
            .get_asset_spec(data)
            .replace_attributes(key=raw_table_key(data.resource.source_name, table), deps=[])
        )


class LakeDbtTranslator(DagsterDbtTranslator):
    """Keys dbt sources as the raw tables dlt loads, so both tools form one graph."""

    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: DbtProject | None
    ) -> dg.AssetSpec:
        resource_props = self.get_resource_props(manifest, unique_id)
        return (
            super()
            .get_asset_spec(manifest, unique_id, project)
            .replace_attributes(key=dbt_resource_key(resource_props))
        )
