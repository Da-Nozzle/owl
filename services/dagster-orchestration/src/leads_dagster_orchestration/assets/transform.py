import json
import os
import shutil
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from leads_dagster_orchestration.translators import LakeDbtTranslator

# A source's models are in models/<layer>/<source>/ for each of these layers.
SOURCE_LAYERS = ("raw", "staging")
SHARED_STEP = "transform"
# The run month a dbt step builds, which the project's run_month() macro reads.
RUN_MONTH_VARIABLE = "LEADS_RUN_MONTH"


def build_transform_assets(
    project: DbtProject, partitions: dg.MonthlyPartitionsDefinition, main_state_dir: Path
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
        with environment_variable(RUN_MONTH_VARIABLE, context.partition_key):
            build = dbt.cli(["build"], context=context)
        yield from (
            build.stream().fetch_column_metadata(with_column_lineage=False).fetch_row_counts()
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
