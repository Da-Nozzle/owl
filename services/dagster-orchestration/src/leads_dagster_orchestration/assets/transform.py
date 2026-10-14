from collections.abc import Iterator

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from leads_dagster_orchestration.translators import LakeDbtTranslator


def build_transform_assets(project: DbtProject) -> dg.AssetsDefinition:
    @dbt_assets(
        manifest=project.manifest_path,
        project=project,
        name="transform",
        pool="transform",
        dagster_dbt_translator=LakeDbtTranslator(),
    )
    def transform_assets(
        context: dg.AssetExecutionContext, dbt: DbtCliResource
    ) -> Iterator[object]:
        yield from dbt.cli(["build"], context=context).stream()

    return transform_assets
