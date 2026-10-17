# Vilnius Shadow-Economy Leads

Monthly pipeline that ranks Vilnius businesses which look busy on Google but declare little to Sodra.
Plan and code standards: [Pipeline Plan](https://claude.ai/code/artifact/2803bbda-8626-4a47-ae98-256daea0289b).

## Local development

Requirements: Docker with Compose v2 and VS Code with the Dev Containers extension.
Everything for local work lives in `.devcontainer/`; Compose is not used outside it.

1. Open the repo in VS Code and choose **Reopen in Container**.
   - On the host, `initializeCommand` builds the dev image `leads-dev:local`.
   - Compose starts the dev container with Postgres, s3mock and Marquez.
   - `post-create.sh` runs `uv sync --frozen`, installing the workspace into the container's system
     Python (3.14.7), and links `dagster.yaml` into `$DAGSTER_HOME`. There is no virtual environment.
     Downloads are cached in the `uv-cache` volume, so rebuilding the container is quick.
2. In the container terminal:

   ```sh
   pytest                      # all tests; -m unit for the in-process ones only (below)
   ruff check . && ruff format --check .
   leads-dbt --lake replay debug  # dbt on a lake (live or replay), secrets resolved from config
   leads-dbt-dev build -s state:modified+  # your model changes, in a dev copy (below)
   leads-dbt-diff              # what those changes did to the tables (below)
   leads-dagster-dev           # Dagster webserver and daemon; UI on http://localhost:3000
   ```

GitHub from the container: `git` over HTTPS uses your host's credentials, which VS Code forwards.
`gh` keeps its own login, so run `gh auth login` once; the `gh-config` volume keeps it across
rebuilds. The `dev` user has passwordless `sudo`.

Without VS Code:

```sh
docker build --tag leads-dev:local .devcontainer
docker compose -f .devcontainer/compose.yml up -d
docker compose -f .devcontainer/compose.yml exec dev sh .devcontainer/post-create.sh
docker compose -f .devcontainer/compose.yml exec dev leads-dagster-dev
```

### What runs where

| Service | Image | Reached at |
|---|---|---|
| `dev` | `python:3.14.7-slim` + uv | container shell; Dagster UI on http://localhost:3000 |
| `postgres` | `postgres:17-alpine` | `postgres:5432` (databases `dagster`, `ducklake_catalog`, `marquez`, `app`) |
| `s3mock` | `adobe/s3mock:5.2.3` | `s3mock:9090`, bucket `leads-lake` |
| `marquez` / `marquez-web` | `marquezproject/marquez(-web):0.51.1` | `marquez:5000`; UI on http://localhost:3001 |

Only the two UIs are published to the host. Every container reaches the others by service name.

### Configuration

Config is TOML in `config/<env>/`, mounted read-only at `/etc/leads/` and read by the
configuration service in `services/config/`. Commented blocks in each file show how production
would differ.

Local dev credentials (Postgres, s3mock) are plain values in the config. Real secrets are
references that resolve when first read; locally they come from files in `config/local/secrets/`,
which is git-ignored. Replay needs none. To run live against the Places API, put the key in
`config/local/secrets/google_places_api_key`.

Logs go to stderr at INFO and to `logs/leads.log` at DEBUG, rotated at 10 MB across 10 files.

Dagster and dbt only accept credentials through environment variables, so they are started through
`leads-dagster-dev` and `leads-dbt`, which ask the configuration service and set those variables for
that one process. Under Dagster, dbt's variables are set by the definitions instead, so
**Reload definitions** applies a configuration change to dbt too.

`leads-dagster-dev` runs `dagster dev` against the repo, so code changes appear after
**Reload definitions** in the Dagster UI.

### Live and replay

Dagster loads two code locations side by side (`.devcontainer/dagster/workspace.yaml`), built by
the same code into separate lakes, so the sample's rows never mix with live ones:

| Location | Reads | Builds into | Asset keys and group |
|---|---|---|---|
| `live` | the sources, fetched into the archive (`lake.archive_path`) | `lake.metadata_schema` | `live/<layer>/<name>`, group `live/<source>` |
| `replay` | the committed sample (`pipeline.replay_path`); calls nothing | `lake.replay` | `replay/<layer>/<name>`, group `replay/<source>` |

Production deploys `live` only. Nothing switches between them: each lake is one box in the
lineage view (its group), holding a box per source (a group nested in the lake's, as Dagster reads
`/` in a group name), fetch → archive → raw → staging left to right inside; models that combine
sources go in the lake's box itself. Each lake is also one folder in the asset catalog, with its
own history and pools. Node names say the layer (`fetch_`, `archive_`, `raw_`, `stg_`); only live
has a fetch, and the archive is drawn dashed, as nothing can materialize it.

### Snapshots

Every source goes through the raw archive. What one fetch of a source captures is a snapshot,
kept as fetched at `<archive>/<source>/<snapshot>/`, and complete once the fetch has written its
manifest, `<archive>/<source>/_snapshots/<snapshot>.json`, last. A snapshot is keyed by the
period it was fetched in, which each source sets with its schedule, as often as the source
publishes:

| Source | Fetches | Schedule | Key |
|---|---|---|---|
| `sodra` | the yearly employer files covering the last 13 months | 21st at 06:00 (`sodra.snapshot_schedule`), as Sodra publishes the previous month by the 20th | `YYYY-MM` |
| `jar` | the register's entities, legal forms and statuses, each whole | 21st at 06:00 (`jar.snapshot_schedule`): the register publishes monthly at most, on no fixed day | `YYYY-MM` |

A source fetched daily would be keyed `YYYY-MM-DD`. A second fetch in the same period replaces that
period's snapshot.

A source's partitions are its complete snapshots, so every partition has something to build from,
and a period that has passed cannot be fetched again: the fetch takes no partition, the clock
picks its snapshot. Adding a month to the replay sample, with its manifest, adds a partition with
no reload.

| What | Does | Started by |
|---|---|---|
| `fetch_<source>` (live) | fetches the source now into the archive, as the current period's snapshot | its schedule, or you |
| `archive_index` sensor | adds a partition for each complete snapshot, and reports it as the archive's | Dagster, every minute |
| `transform_automation` sensor | builds raw for each new snapshot, then staging and after; rebuilds staging and after when their SQL changes | Dagster |
| `rebuild_<source>_from_archive` | rebuilds raw for the snapshots you choose, then staging and after once | you, from the job's page (a backfill for several) |

So each source's monthly build is a chain of runs: the scheduled fetch, then (within a minute or
two) raw for the new snapshot, then staging. Sources build side by side, each in its own pools.
Raw is partitioned by snapshot; staging and later models are one table each, rebuilt whole, so
their latest materialization is the table's.

Every run and materialization is labelled `leads/trigger`: `schedule` or `fetch` (by hand) on a
fetch, `automation` on what Dagster built by itself, `rebuild` on a rebuild by hand. Each raw
snapshot also shows `archive_fetched_at`, the download it was built from. Filter the runs list by
`leads/trigger` to see, say, every rebuild.

A new source is one `ArchivedSource` in `archived_sources()` (`entrypoints/definitions.py`): its
name, its schedule and key format, and its fetch (`leads_ingest/sources/<source>/`, with its
settings in `config/<env>/pipeline.toml`), plus its dbt models in `models/raw/<source>/` (schema
`raw_<source>` in `dbt_project.yml`) and `models/staging/<source>/`. Its partitions, archive,
fetch job and schedule, rebuild job, automation and group all follow from that. `jar` is the
smallest example: three CSV exports, read by the `archived_csv` macro.

### Tests

Each package keeps its tests in `tests/unit/` (in-process only) or `tests/integration/` (a temporary
directory, DuckDB, a local HTTP server, or a throwaway Dagster instance running real runs; none
needs the compose services). `conftest.py` marks each test by its directory, so `pytest -m unit` or
`pytest -m integration` runs one kind, and a test in neither directory fails collection.

Tests build settings with `leads_testing` (`libs/testing`, a dev dependency only):
`build_config({"sodra.history_months": 1})` is a configuration like `config/local` with the dotted keys
replaced, and `leads_testing.ingest` holds a `Download` builder and in-memory fakes of the ingest
ports. dbt's own tests stay in `services/transform/tests/`, where dbt reads them.

### Iterating on models

Load the sources once through Dagster, then work on the SQL with `leads-dbt-dev`. It runs dbt on the
`dev` target: models build into a DuckDB file of their own, and a lake is attached read-only, so
nothing you do there touches the lake or waits for a Dagster run. A model you do not select is read
from the lake (dbt's `--defer`), as that location's last build left it. `--lake` picks the lake,
`replay` unless given; each lake has its own dev copy in `services/transform/target/<lake>/dev/`.

```sh
leads-dbt-dev build -s state:modified+                # what you changed since Dagster's last build
leads-dbt-dev build -s stg_sodra__monthly+            # one model and everything after it
leads-dbt-dev show -s stg_sodra__monthly --limit 20   # look at the result
leads-dbt-dev build -s raw_sodra__monthly            # raw, read from the archive
leads-dbt-dev --lake live build -s state:modified+   # the same against the live lake
```

Then `leads-dbt-diff` compares each model the last `leads-dbt-dev` command built with the lake's
table: rows on one side only, columns added, dropped or retyped, and on a model whose meta names
its `key` (staging: `insurer_code, declared_month`), how many rows changed in each column, with
examples. A change should show only what it meant to: adding a column ends in `only columns
added`; anything else ends in `CHANGED`. `leads-dbt-diff stg_sodra__monthly` names models instead,
and `--lake` works as above. It compares what is built in dev, so rebuild after editing the SQL.

```text
stg_sodra__monthly: "dev"."staging"."stg_sodra__monthly" (dev) against "lake"."staging"."stg_sodra__monthly" (lake), matched on insurer_code, declared_month
  columns added:   insured_count_2 INTEGER
  columns dropped: none
  columns retyped: none
  rows: 30,352 in the lake, 30,352 in dev; 0 only in the lake, 0 only in dev
  values changed in matched rows: none
  => only columns added
```

Once merged, **Reload definitions** (a deploy, in production) is all that is left: the models whose
SQL changed get a new code version, and the `transform_automation` sensor rebuilds them and
everything after them. A new model is built by the next new snapshot, or by hand.

Raw models read the archive folder of a snapshot. On `leads-dbt-dev` that is the source's newest
complete snapshot unless `LEADS_SNAPSHOT=<key>` names another; `leads-dbt` always needs it, and
Dagster sets it per partition.

Once a model is built in dev, the models after it read the dev copy, not the lake's. To read the
lake's again, add `--favor-state`, or start over by deleting `target/<lake>/dev/`.
