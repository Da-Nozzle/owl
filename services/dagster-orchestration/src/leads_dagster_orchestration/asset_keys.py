from collections.abc import Mapping

import dagster as dg

from leads_ingest.settings import PipelineMode

FETCH_LAYER = "fetch"
PLAN_LAYER = "plan"
ARCHIVE_LAYER = "archive"
RAW_LAYER = "raw"
RAW_SCHEMA_PREFIX = "raw_"
STATIC_DATA = "static_data"

# Keys are <lake>/<layer>/<name> (live/raw/raw_sodra__monthly): Dagster keeps one node per key
# across code locations, so the lake comes first. The UI labels a node with the last part only,
# so the name repeats the layer, as dbt model names do.


def fetch_key(source: str, lake: PipelineMode) -> dg.AssetKey:
    return dg.AssetKey([lake.value, FETCH_LAYER, f"{FETCH_LAYER}_{source}"])


def plan_key(source: str, lake: PipelineMode) -> dg.AssetKey:
    return dg.AssetKey([lake.value, PLAN_LAYER, f"{PLAN_LAYER}_{source}"])


def archive_key(source: str, lake: PipelineMode) -> dg.AssetKey:
    return dg.AssetKey([lake.value, ARCHIVE_LAYER, f"{ARCHIVE_LAYER}_{source}"])


def transform_model_key(layer: str, model: str, lake: PipelineMode) -> dg.AssetKey:
    return dg.AssetKey([lake.value, layer, model])


def lake_group(lake: PipelineMode) -> str:
    """One group per lake: Dagster merges groups of the same name across code locations."""
    return lake.value


def source_group(group: str, lake: PipelineMode) -> str:
    """Nested in the lake's group, as Dagster reads `/` as nesting. `group` is the source's name,
    or a path for sources drawn together (`vmi/daily_changes`)."""
    return f"{lake_group(lake)}/{group}"


def static_data_group(lake: PipelineMode) -> str:
    """The lake's seeds: data kept by hand in the repository."""
    return f"{lake_group(lake)}/{STATIC_DATA}"


def is_archive(key: dg.AssetKey) -> bool:
    return len(key.path) >= 2 and key.path[-2] == ARCHIVE_LAYER


def dbt_resource_key(dbt_resource_props: Mapping[str, object], lake: PipelineMode) -> dg.AssetKey:
    """A model's layer is its schema, every raw_<source> schema being the raw layer. The only dbt
    source is the archive: nothing but dbt writes to the lake."""
    name = str(dbt_resource_props["name"])
    if dbt_resource_props["resource_type"] == "source":
        source_name = str(dbt_resource_props["source_name"])
        if source_name != ARCHIVE_LAYER:
            raise ValueError(
                f"dbt source {source_name}.{name} is not the archive: every source is read from "
                f"the archive ({ARCHIVE_LAYER}.<source>)"
            )
        return archive_key(name, lake)
    config = dbt_resource_props.get("config")
    schema = config.get("schema") if isinstance(config, Mapping) else None
    if not isinstance(schema, str):
        raise ValueError(f"dbt resource {name!r} has no +schema, so it has no asset key layer")
    layer = RAW_LAYER if schema.startswith(RAW_SCHEMA_PREFIX) else schema
    return transform_model_key(layer, name, lake)
