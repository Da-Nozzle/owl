from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtProject
from dagster_dlt import DagsterDltTranslator
from dagster_dlt.translator import DltResourceTranslatorData

from leads_dagster_orchestration.asset_keys import (
    dbt_resource_key,
    raw_table_key,
    source_group,
    static_data_group,
)
from leads_ingest.settings import PipelineMode


class RawTableDltTranslator(DagsterDltTranslator):
    """Keys each dlt resource as <lake>/raw/raw_<source>__<table>, in its source's group, with no
    upstream assets."""

    def __init__(self, lake: PipelineMode) -> None:
        super().__init__()
        self._lake = lake

    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        table = data.resource.table_name
        if not isinstance(table, str):
            raise TypeError(f"dlt resource {data.resource.name!r} has a dynamic table name")
        source = data.resource.source_name
        key = raw_table_key(source, table, self._lake)
        return (
            super()
            .get_asset_spec(data)
            .replace_attributes(key=key, group_name=source_group(source, self._lake), deps=[])
        )


class LakeDbtTranslator(DagsterDbtTranslator):
    """Keys dbt resources so that dbt, dlt and the archive form one graph, under the lake's key
    prefix. A step's models get its group (its source's, within the lake's) and its automation
    condition, if any; its seeds, the lake's static data group."""

    def __init__(
        self,
        lake: PipelineMode,
        group: str,
        automation_condition: dg.AutomationCondition | None = None,
    ) -> None:
        super().__init__()
        self._lake = lake
        self._group = group
        self._automation_condition = automation_condition

    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: DbtProject | None
    ) -> dg.AssetSpec:
        props = self.get_resource_props(manifest, unique_id)
        key = dbt_resource_key(props, self._lake)
        group = static_data_group(self._lake) if props["resource_type"] == "seed" else self._group
        return (
            super()
            .get_asset_spec(manifest, unique_id, project)
            .replace_attributes(key=key, group_name=group)
        )

    def get_automation_condition(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> dg.AutomationCondition | None:
        return self._automation_condition
