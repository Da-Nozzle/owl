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
   leads-dbt debug             # dbt with the DuckLake profile, secrets resolved from config
   leads-dbt-dev build -s state:modified+  # your model changes, in a dev copy (below)
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
which is git-ignored. Replay mode needs none. To run live against the Places API, put the key in
`config/local/secrets/google_places_api_key` and set `[pipeline] mode = "live"`. Replay and live
build into separate lakes (`[lake.replay]`), so switching modes never mixes the sample's rows with
live ones. A live run downloads only for the current run month; rebuild a past one from the
archive by materializing only the assets downstream of `archive/archive_sodra`.

Logs go to stderr at INFO and to `logs/leads.log` at DEBUG, rotated at 10 MB across 10 files.

Dagster and dbt only accept credentials through environment variables, so they are started through
`leads-dagster-dev` and `leads-dbt`, which ask the configuration service and set those variables for
that one process. Under Dagster, dbt's variables are set by the definitions instead, so
**Reload definitions** applies a configuration change to dbt too.

`leads-dagster-dev` runs `dagster dev` against the repo, so code changes appear after
**Reload definitions** in the Dagster UI.

### Tests

Each package keeps its tests in `tests/unit/` (in-process only) or `tests/integration/` (a temporary
directory, DuckDB, a local HTTP server, or a throwaway Dagster instance running real runs; none
needs the compose services). `conftest.py` marks each test by its directory, so `pytest -m unit` or
`pytest -m integration` runs one kind, and a test in neither directory fails collection.

Tests build settings with `leads_testing` (`libs/testing`, a dev dependency only):
`build_config({"pipeline.run_day": 29})` is a configuration like `config/local` with the dotted keys
replaced, and `leads_testing.ingest` holds a `Download` builder and in-memory fakes of the ingest
ports. dbt's own tests stay in `services/transform/tests/`, where dbt reads them.

### Iterating on models

Load the sources once through Dagster, then work on the SQL with `leads-dbt-dev`. It runs dbt on the
`dev` target: models build into `services/transform/target/dev/dev.duckdb`, and the lake is
attached read-only, so nothing you do there touches the lake or waits for a Dagster run. A model
you do not select is read from the lake (dbt's `--defer`), as Dagster's last build left it.

```sh
leads-dbt-dev build -s state:modified+                # what you changed since Dagster's last build
leads-dbt-dev build -s stg_sodra__monthly+            # one model and everything after it
leads-dbt-dev show -s stg_sodra__monthly --limit 20   # look at the result
leads-dbt-dev build -s raw_sodra__monthly            # raw, read from the archive
```

Raw models read the archive folder of a run month. On `leads-dbt-dev` that is the newest run month
in the archive unless `LEADS_RUN_MONTH=YYYY-MM` names another; `leads-dbt` always needs it, and
Dagster sets it per partition.

Once a model is built in dev, the models after it read the dev copy, not the lake's. To read the
lake's again, add `--favor-state`, or start over by deleting `target/dev/`.
