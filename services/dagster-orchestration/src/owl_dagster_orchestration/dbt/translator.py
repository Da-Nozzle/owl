from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtProject

from owl_dagster_orchestration.asset_keys import (
    dbt_resource_key,
    source_group,
    static_data_group,
)
from owl_dagster_orchestration.automation import build_static_data
from owl_ingest.settings import PipelineMode


class LakeDbtTranslator(DagsterDbtTranslator):
    """Keys dbt resources into the lake's graph, joined to the archive. A step's models get its
    group and automation condition; seeds, the lake's static data group and a condition of their
    own. With `group_by_folder`, a model in a folder of its layer (`models/intermediate/google/`)
    is drawn in that folder's group within the lake's, as a staging folder of no source's name
    is."""

    def __init__(
        self,
        lake: PipelineMode,
        group: str,
        automation_condition: dg.AutomationCondition | None = None,
        *,
        group_by_folder: bool = False,
    ) -> None:
        super().__init__()
        self._lake = lake
        self._group = group
        self._automation_condition = automation_condition
        self._group_by_folder = group_by_folder

    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: DbtProject | None
    ) -> dg.AssetSpec:
        props = self.get_resource_props(manifest, unique_id)
        key = dbt_resource_key(props, self._lake)
        return (
            super()
            .get_asset_spec(manifest, unique_id, project)
            .replace_attributes(key=key, group_name=self._resource_group(props))
        )

    def _resource_group(self, props: Mapping[str, Any]) -> str:
        if props["resource_type"] == "seed":
            return static_data_group(self._lake)
        fqn = props["fqn"]  # [project, layer, folder..., model]
        if self._group_by_folder and len(fqn) >= 4:
            return source_group(fqn[2], self._lake)
        return self._group

    def get_automation_condition(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> dg.AutomationCondition | None:
        if dbt_resource_props["resource_type"] == "seed":
            return build_static_data()
        return self._automation_condition
