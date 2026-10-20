#!/bin/sh
# What CI checks, run in the dev container against its compose services (.github/workflows/ci.yml).
# Run it here to check a branch the same way.
set -eu

cd "$(dirname "$0")/.."

uv lock --check
ruff check .
ruff format --check .
pytest --cov
