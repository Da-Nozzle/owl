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
   pytest                      # unit tests
   ruff check . && ruff format --check .
   leads-dbt debug             # dbt with the DuckLake profile, secrets resolved from config
   leads-dagster-dev           # Dagster webserver and daemon; UI on http://localhost:3000
   ```

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
`config/local/secrets/google_places_api_key` and set `[pipeline] mode = "live"`.

Logs go to stderr at INFO and to `logs/leads.log` at DEBUG, rotated at 10 MB across 10 files.

Dagster and dbt only accept credentials through environment variables, so they are started through
`leads-dagster-dev` and `leads-dbt`, which ask the configuration service and set those variables for
that one process.

`leads-dagster-dev` runs `dagster dev` against the repo, so code changes appear after
**Reload definitions** in the Dagster UI.
