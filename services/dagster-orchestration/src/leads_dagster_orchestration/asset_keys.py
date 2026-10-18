from collections.abc import Mapping

import dagster as dg

from leads_ingest.settings import PipelineMode

FETCH_LAYER = "fetch"
PLAN_LAYER = "plan"
ARCHIVE_LAYER = "archive"
RAW_LAYER = "raw"
RAW_SCHEMA_PREFIX = "raw_"
STATIC_DATA = "static_data"

# Every asset is keyed <lake>/<layer>/<name>, e.g. live/raw/raw_sodra__monthly and
# replay/staging/stg_sodra__monthly. The lake comes first: the two lakes' tables are different
# tables, Dagster keeps one node per key across code locations, and the asset catalog shows one
# folder per lake. The UI labels a node with only the last part of its key and has no display
# name, so that part repeats the layer as a prefix (archive_, raw_, stg_, ...) with the source,
# as dbt model names already do. A source flows fetch (live only) -> archive -> raw -> staging;
# a source billed per request has a plan before its fetch (live only), what the fetch pays for.


def fetch_key(source: str, lake: PipelineMode) -> dg.AssetKey:
    return dg.AssetKey([lake.value, FETCH_LAYER, f"{FETCH_LAYER}_{source}"])


def plan_key(source: str, lake: PipelineMode) -> dg.AssetKey:
    return dg.AssetKey([lake.value, PLAN_LAYER, f"{PLAN_LAYER}_{source}"])


def archive_key(source: str, lake: PipelineMode) -> dg.AssetKey:
    return dg.AssetKey([lake.value, ARCHIVE_LAYER, f"{ARCHIVE_LAYER}_{source}"])


def transform_model_key(layer: str, model: str, lake: PipelineMode) -> dg.AssetKey:
    return dg.AssetKey([lake.value, layer, model])


def lake_group(lake: PipelineMode) -> str:
    """Each lake's assets are one Dagster group, which the lineage view draws as one box. Dagster
    merges groups of the same name across code locations, so grouped by layer, each layer's box
    held both lakes' nodes. Models that combine sources are in this group itself."""
    return lake.value


def source_group(group: str, lake: PipelineMode) -> str:
    """A source's assets, fetch to staging, are a group nested in its lake's: Dagster reads `/`
    in a group name as nesting, and draws `live/sodra` as a box `sodra`, its layers flowing left
    to right inside, within the box `live`. `group` is the source's name, or a path for sources
    drawn together (`vmi/daily_changes`, in the box `vmi`)."""
    return f"{lake_group(lake)}/{group}"


def static_data_group(lake: PipelineMode) -> str:
    """The lake's seeds, drawn as a box of their own within the lake's: data kept by hand in the
    repository (reference tables, category mappings, the scope of Google's search), which no
    pipeline updates."""
    return f"{lake_group(lake)}/{STATIC_DATA}"


def is_archive(key: dg.AssetKey) -> bool:
    return len(key.path) >= 2 and key.path[-2] == ARCHIVE_LAYER


def dbt_resource_key(dbt_resource_props: Mapping[str, object], lake: PipelineMode) -> dg.AssetKey:
    """The `archive` source is keyed as the archive asset that fills it. A model is keyed
    <lake>/<layer>/<model name>, its layer being the lake schema it builds into, with every
    raw_<source> schema in the raw layer. Nothing but dbt writes to the lake, so a dbt source
    other than the archive would be a table no asset fills, and is rejected."""
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
