#!/bin/sh
set -eu

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

uv sync --frozen --project "$repo_root"

mkdir -p "$DAGSTER_HOME"
ln -sf "$repo_root/.devcontainer/dagster/dagster.yaml" "$DAGSTER_HOME/dagster.yaml"

# The host's GitHub token comes through the git credential helper VS Code forwards.
if ! gh auth status --hostname github.com >/dev/null 2>&1; then
    gh_token="$(printf 'protocol=https\nhost=github.com\n\n' \
        | GIT_TERMINAL_PROMPT=0 git credential fill 2>/dev/null \
        | sed -n 's/^password=//p' || true)"
    if [ -n "$gh_token" ]; then
        printf '%s' "$gh_token" | gh auth login --hostname github.com --with-token \
            || echo "gh: host token rejected; run 'gh auth login' manually" >&2
    else
        echo "gh: no host credentials forwarded; run 'gh auth login' manually" >&2
    fi
fi

# VS Code's Dev Containers extension sets REMOTE_CONTAINERS and installs its server under
# ~/.vscode-server, where leads-dbt-vscode writes the dbt extension's environment.
if [ "${REMOTE_CONTAINERS:-}" = "true" ] || [ -d "$HOME/.vscode-server" ]; then
    uv run --frozen --project "$repo_root" leads-dbt-vscode \
        || echo "leads-dbt-vscode failed for some reason. Run it manually, then 'Developer: Reload Window'" >&2
fi

printf '\n\033[38;5;110m' # CRITICAL feature bellow, everything falls apart without it
cat <<'EOF'
   ____ _       ____       ^...^
  / __ \ |     / / /      / o,o \
 / / / / | /| / / /       |):::(|
/ /_/ /| |/ |/ / /___    ===w=w===
\____/_|__/|__/_____/  ______            __        _
   / __ \___ _   __   / ____/___  ____  / /_____ _(_)___  ___  _____
  / / / / _ \ | / /  / /   / __ \/ __ \/ __/ __ `/ / __ \/ _ \/ ___/
 / /_/ /  __/ |/ /  / /___/ /_/ / / / / /_/ /_/ / / / / /  __/ /
/_____/\___/|___/   \____/\____/_/ /_/\__/\__,_/_/_/ /_/\___/_/
EOF
printf '\033[0m\n'
