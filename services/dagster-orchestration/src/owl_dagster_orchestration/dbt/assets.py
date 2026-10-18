import json
from collections.abc import Iterator, Mapping
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from owl_dagster_orchestration.asset_keys import lake_group, source_group
from owl_dagster_orchestration.automation import build_new_snapshots, rebuild_on_change
from owl_dagster_orchestration.dbt.environment import SNAPSHOT_VARIABLE, environment_variable
from owl_dagster_orchestration.dbt.project import keep_main_state
from owl_dagster_orchestration.dbt.step_metadata import snapshot_metadata, with_compiled_sql
from owl_dagster_orchestration.dbt.translator import LakeDbtTranslator
from owl_dagster_orchestration.lake_labels import label_run, with_labels
from owl_ingest.settings import PipelineMode

# A source's models are in models/<layer>/<source>/.
SOURCE_LAYERS = ("raw", "staging")
# The only layer partitioned by snapshot, one slice each; later layers are rebuilt whole.
SNAPSHOT_LAYER = "raw"
# Layers whose models read archived files (read_json, read_csv), not tables: sqlglot cannot
# resolve their columns, and they have no parent columns to trace.
FILE_LAYERS = ("raw",)
SHARED_STEP = "transform"


def build_transform_assets(
    project: DbtProject,
    snapshot_partitions: Mapping[str, dg.PartitionsDefinition],
    main_state_dir: Path,
    labels: Mapping[str, str],
    lake: PipelineMode,
    groups: Mapping[str, str] | None = None,
) -> list[dg.AssetsDefinition]:
    """Per source, a raw and a staging step in one pool, so no source's tables are written twice
    at once; plus a shared step for the rest. `groups` maps a source folder to its group path."""
    manifest = json.loads(project.manifest_path.read_text())

    def step(
        name: str,
        select: str,
        *,
        pool: str,
        group: str,
        exclude: str | None = None,
        partitions: dg.PartitionsDefinition | None = None,
        column_lineage: bool = True,
    ) -> dg.AssetsDefinition:
        return build_dbt_step(
            project,
            main_state_dir,
            labels,
            lake,
            name=name,
            select=select,
            exclude=exclude,
            pool=pool_name(lake, pool),
            group=group,
            partitions=partitions,
            column_lineage=column_lineage,
        )

    steps = []
    for source, layers in source_layers(manifest).items():
        pool = f"{SHARED_STEP}_{source}"
        if SNAPSHOT_LAYER in layers and source not in snapshot_partitions:
            raise ValueError(
                f"dbt models in models/{SNAPSHOT_LAYER}/{source}/ have no archived source named "
                f"{source!r}: add it to the code location's sources"
            )
        steps.extend(
            step(
                f"{pool}_{layer}",
                f"path:models/{layer}/{source}",
                pool=pool,
                group=source_group((groups or {}).get(source, source), lake),
                partitions=snapshot_partitions[source] if layer == SNAPSHOT_LAYER else None,
                column_lineage=layer not in FILE_LAYERS,
            )
            for layer in SOURCE_LAYERS
            if layer in layers
        )
    if has_shared_models(manifest):
        steps.append(
            step(
                SHARED_STEP,
                "path:models path:seeds",
                pool=SHARED_STEP,
                group=lake_group(lake),
                exclude=" ".join(f"path:models/{layer}" for layer in SOURCE_LAYERS),
            )
        )
    return steps


def pool_name(lake: PipelineMode, pool: str) -> str:
    """Pools are instance-wide: neither lake queues behind the other."""
    return f"{lake.value}_{pool}"


def build_dbt_step(
    project: DbtProject,
    main_state_dir: Path,
    labels: Mapping[str, str],
    lake: PipelineMode,
    *,
    name: str,
    select: str,
    pool: str,
    group: str,
    partitions: dg.PartitionsDefinition | None,
    exclude: str | None = None,
    column_lineage: bool = True,
) -> dg.AssetsDefinition:
    @dbt_assets(
        manifest=project.manifest_path,
        project=project,
        select=select,
        exclude=exclude,
        name=name,
        partitions_def=partitions,
        backfill_policy=(
            dg.BackfillPolicy.multi_run(max_partitions_per_run=1) if partitions else None
        ),
        pool=pool,
        dagster_dbt_translator=LakeDbtTranslator(
            lake,
            group,
            automation_condition=build_new_snapshots() if partitions else rebuild_on_change(),
        ),
    )
    def dbt_step(context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Iterator[object]:
        run_labels = label_run(context, labels)
        # An unpartitioned step in a partitioned run still sees the run's partition key.
        snapshot = context.partition_key if partitions else None
        with environment_variable(SNAPSHOT_VARIABLE, snapshot) if snapshot else nullcontext():
            build = dbt.cli(["build"], context=context)
        events = (
            build.stream()
            .fetch_column_metadata(with_column_lineage=column_lineage)
            .fetch_row_counts()
        )
        for event in events:
            counted = snapshot_metadata(event, build, snapshot) if snapshot else event
            yield with_labels(with_compiled_sql(counted, build), run_labels)
        keep_main_state(build.target_path / "manifest.json", main_state_dir)

    return dbt_step


def source_layers(manifest: Mapping[str, Any]) -> dict[str, set[str]]:
    """Each source's layers that hold a model of it, by source folder name, sorted by source."""
    layers: dict[str, set[str]] = {}
    for node in manifest["nodes"].values():
        fqn = node["fqn"]  # [project, layer, folder..., model]
        if node["resource_type"] != "model" or fqn[1] not in SOURCE_LAYERS:
            continue
        if len(fqn) < 4:
            raise ValueError(
                f"{node['original_file_path']} is not in a source folder: "
                f"put it in models/{fqn[1]}/<source>/"
            )
        layers.setdefault(fqn[2], set()).add(fqn[1])
    return dict(sorted(layers.items()))


def has_shared_models(manifest: Mapping[str, Any]) -> bool:
    """Whether the project has a seed or a model outside the source layers."""
    return any(
        node["resource_type"] == "seed"
        or (node["resource_type"] == "model" and node["fqn"][1] not in SOURCE_LAYERS)
        for node in manifest["nodes"].values()
    )
