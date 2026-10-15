import json
import os
import shutil
import uuid
from collections.abc import Iterator, Mapping
from contextlib import chdir, contextmanager
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
from dagster._core.definitions.definitions_load_context import (
    DefinitionsLoadContext,
    DefinitionsLoadType,
)
from dagster_dbt import DbtCliInvocation, DbtCliResource, DbtProject, dbt_assets

from leads_dagster_orchestration.lake_labels import label_run, with_labels
from leads_dagster_orchestration.translators import LakeDbtTranslator

# A source's models are in models/<layer>/<source>/ for each of these layers.
SOURCE_LAYERS = ("raw", "staging")
SHARED_STEP = "transform"
# The run month a dbt step builds, which the project's run_month() macro reads.
RUN_MONTH_VARIABLE = "LEADS_RUN_MONTH"
# dbt meta naming the column of a model that keeps one slice per run month (raw tables).
RUN_MONTH_COLUMN_META = "run_month_column"
ROW_COUNT = "dagster/row_count"
TABLE_ROW_COUNT = "table_row_count"


def build_transform_assets(
    project: DbtProject,
    partitions: dg.MonthlyPartitionsDefinition,
    main_state_dir: Path,
    labels: Mapping[str, str],
) -> list[dg.AssetsDefinition]:
    """The dbt project as one step per source, plus one for the models that combine sources.
    A source's step, `transform_<source>`, builds its raw and staging models in a pool of the
    same name: sources build side by side, and two run months of one source never write its
    tables at once. The shared step, `transform` (intermediate, marts, seeds), exists once the
    project has such a model. Each step is one dbt invocation, so models within it still run on
    dbt's threads."""
    manifest = json.loads(project.manifest_path.read_text())
    steps = [
        build_dbt_step(
            project,
            partitions,
            main_state_dir,
            labels,
            name=f"{SHARED_STEP}_{source}",
            select=" ".join(f"path:models/{layer}/{source}" for layer in SOURCE_LAYERS),
        )
        for source in dbt_sources(manifest)
    ]
    if has_shared_models(manifest):
        steps.append(
            build_dbt_step(
                project,
                partitions,
                main_state_dir,
                labels,
                name=SHARED_STEP,
                select="path:models path:seeds",
                exclude=" ".join(f"path:models/{layer}" for layer in SOURCE_LAYERS),
            )
        )
    return steps


def build_dbt_step(
    project: DbtProject,
    partitions: dg.MonthlyPartitionsDefinition,
    main_state_dir: Path,
    labels: Mapping[str, str],
    *,
    name: str,
    select: str,
    exclude: str | None = None,
) -> dg.AssetsDefinition:
    @dbt_assets(
        manifest=project.manifest_path,
        project=project,
        select=select,
        exclude=exclude,
        name=name,
        partitions_def=partitions,
        backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
        pool=name,
        dagster_dbt_translator=LakeDbtTranslator(),
    )
    def dbt_step(context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Iterator[object]:
        label_run(context, labels)
        with environment_variable(RUN_MONTH_VARIABLE, context.partition_key):
            build = dbt.cli(["build"], context=context)
        events = build.stream().fetch_column_metadata(with_column_lineage=False).fetch_row_counts()
        yield from (
            with_labels(count_run_month_rows(event, build, context.partition_key), labels)
            for event in events
        )
        keep_main_state(build.target_path / "manifest.json", main_state_dir)

    return dbt_step


@contextmanager
def environment_variable(name: str, value: str) -> Iterator[None]:
    """Sets a variable for the dbt process that dagster-dbt starts in this block, which takes its
    environment from this process's. The run month goes to dbt this way, not as --vars: dbt
    re-parses the whole project whenever --vars differs from the parse Dagster made at startup,
    which no month's run matches. A step runs in a process of its own, or in-process one after
    another, so no other step sees the variable."""
    environment = os.environ  # noqa: TID251 - sets a variable for dbt; reads no config
    previous = environment.get(name)
    environment[name] = value
    try:
        yield
    finally:
        if previous is None:
            del environment[name]
        else:
            environment[name] = previous


def count_run_month_rows[Event](event: Event, build: DbtCliInvocation, run_month: str) -> Event:
    """dagster-dbt's row count is the whole table's. A model that keeps one slice per run month
    (its dbt meta names the column) grows by a slice each run, so its partition's row count is
    the run month's rows, and the table's is kept as table_row_count."""
    if not isinstance(event, dg.Output | dg.AssetMaterialization):
        return event
    unique_id = event.metadata.get("unique_id")
    node = build.manifest["nodes"].get(unique_id.value if unique_id else None)
    query = run_month_count_query(node, run_month) if node else None
    if query is None or build.adapter is None:
        return event
    with chdir(build.project_dir), build.adapter.connection_named(f"run_month_rows_{node['name']}"):
        _, result = build.adapter.execute(query, fetch=True)
    table_rows = event.metadata.get(ROW_COUNT)
    return event.with_metadata(
        {
            **event.metadata,
            ROW_COUNT: dg.MetadataValue.int(result[0][0]),
            **({TABLE_ROW_COUNT: table_rows} if table_rows is not None else {}),
        }
    )


def run_month_count_query(node: Mapping[str, Any], run_month: str) -> str | None:
    """Counts a run month's rows of a model whose dbt meta names its run month column."""
    column = node.get("meta", {}).get(RUN_MONTH_COLUMN_META)
    if column is None:
        return None
    return f"select count(*) from {node['relation_name']} where \"{column}\" = '{run_month}'"


def set_dbt_environment(variables: Mapping[str, str]) -> None:
    """Sets the values the dbt project reads through env_var() in this process, which the parse
    made when the definitions load and every dbt step pass on to dbt. Set where the definitions
    read the configuration, so a reload moves both to a changed configuration together: dbt's
    partial parse then re-parses once, at the reload, and never in a run, which it would whenever
    a run's values differ from those the saved parse recorded."""
    environment = os.environ  # noqa: TID251 - sets variables for dbt; reads no config
    environment.update(variables)


def dbt_sources(manifest: Mapping[str, Any]) -> list[str]:
    """The sources with models in a source layer, named by their folder."""
    sources = set()
    for node in manifest["nodes"].values():
        fqn = node["fqn"]  # [project, layer, folder..., model]
        if node["resource_type"] != "model" or fqn[1] not in SOURCE_LAYERS:
            continue
        if len(fqn) < 4:
            raise ValueError(
                f"{node['original_file_path']} is not in a source folder: "
                f"put it in models/{fqn[1]}/<source>/"
            )
        sources.add(fqn[2])
    return sorted(sources)


def has_shared_models(manifest: Mapping[str, Any]) -> bool:
    """Whether the project has a seed or a model outside the source layers."""
    return any(
        node["resource_type"] == "seed"
        or (node["resource_type"] == "model" and node["fqn"][1] not in SOURCE_LAYERS)
        for node in manifest["nodes"].values()
    )


def keep_main_state(manifest: Path, state_dir: Path) -> None:
    """Keeps the manifest of a successful build as main's state, which dbt on the dev target
    defers to. Replaced in one step, through a file of its own, so neither a dev command nor
    another dbt step finishing at the same time sees half a file."""
    state_dir.mkdir(parents=True, exist_ok=True)
    partial = state_dir / f"manifest.json.{uuid.uuid4().hex}.partial"
    shutil.copyfile(manifest, partial)
    partial.replace(state_dir / "manifest.json")


def ensure_metadata_database(path: Path) -> None:
    """Creates the empty DuckDB file that Dagster's own process opens, read-only, to read each
    built table's columns and row count: DuckDB opens neither memory nor a missing file
    read-only. The tables themselves are in the lake, which the profile attaches."""
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.{uuid.uuid4().hex}.partial")
    duckdb.connect(str(partial)).close()
    partial.replace(path)


def prepare_on_initial_load(project: DbtProject) -> None:
    """Parses the dbt project under `dagster dev` when the code server loads the definitions, at
    startup and on every reload, but not when a run or one of its steps rebuilds them from that
    load. Each of those is a new process, and the parse (`dbt --version`, an in-process adapter
    and `dbt parse`) cost about 11 s in each, twice per run. A run now uses the manifest of the
    load it was launched from, so its assets are the ones that load showed; dbt still runs the
    project's current files."""
    if DefinitionsLoadContext.get().load_type is DefinitionsLoadType.INITIALIZATION:
        project.prepare_if_dev()
