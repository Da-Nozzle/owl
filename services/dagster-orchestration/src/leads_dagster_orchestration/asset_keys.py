from collections.abc import Mapping

import dagster as dg

RAW_LAYER = "raw"


def raw_table_key(source: str, table: str) -> dg.AssetKey:
    return dg.AssetKey([RAW_LAYER, source, table])


def transform_model_key(layer: str, model: str) -> dg.AssetKey:
    return dg.AssetKey([layer, model])


def dbt_resource_key(dbt_resource_props: Mapping[str, object]) -> dg.AssetKey:
    """Sources are keyed as the raw tables dlt loads; models by the lake schema they build into."""
    name = str(dbt_resource_props["name"])
    if dbt_resource_props["resource_type"] == "source":
        return raw_table_key(str(dbt_resource_props["source_name"]), name)
    config = dbt_resource_props.get("config")
    layer = config.get("schema") if isinstance(config, Mapping) else None
    if not isinstance(layer, str):
        raise ValueError(f"dbt resource {name!r} has no +schema, so it has no asset key layer")
    return transform_model_key(layer, name)
