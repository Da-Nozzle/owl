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
