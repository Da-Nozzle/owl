from collections.abc import Mapping

import dagster as dg

RAW_LAYER = "raw"
ARCHIVE_LAYER = "archive"
RAW_SCHEMA_PREFIX = "raw_"

# Every asset is keyed <layer>/<name>, and the name repeats the layer as a prefix (archive_, raw_,
# stg_, ...) with the source: the UI labels a node with only the last part of its key, and dbt
# model names already follow this, e.g. raw_sodra__monthly and stg_sodra__monthly.


def raw_table_key(source: str, table: str) -> dg.AssetKey:
    return dg.AssetKey([RAW_LAYER, f"{RAW_LAYER}_{source}__{table}"])


def archive_key(source: str) -> dg.AssetKey:
    return dg.AssetKey([ARCHIVE_LAYER, f"{ARCHIVE_LAYER}_{source}"])


def transform_model_key(layer: str, model: str) -> dg.AssetKey:
    return dg.AssetKey([layer, model])


def layer_group(key: dg.AssetKey) -> str:
    """Assets are grouped by layer (archive, raw, staging, ...), the first part of their key."""
    return key.path[0]


def dbt_resource_key(dbt_resource_props: Mapping[str, object]) -> dg.AssetKey:
    """The `archive` source is keyed as the archive asset that fills it, other sources as the
    raw tables dlt loads. A model is keyed <layer>/<model name>, its layer being the lake schema
    it builds into, with every raw_<source> schema in the raw layer."""
    name = str(dbt_resource_props["name"])
    if dbt_resource_props["resource_type"] == "source":
        source_name = str(dbt_resource_props["source_name"])
        if source_name == ARCHIVE_LAYER:
            return archive_key(name)
        return raw_table_key(source_name, name)
    config = dbt_resource_props.get("config")
    schema = config.get("schema") if isinstance(config, Mapping) else None
    if not isinstance(schema, str):
        raise ValueError(f"dbt resource {name!r} has no +schema, so it has no asset key layer")
    layer = RAW_LAYER if schema.startswith(RAW_SCHEMA_PREFIX) else schema
    return transform_model_key(layer, name)
