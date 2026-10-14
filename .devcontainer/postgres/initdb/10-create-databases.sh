#!/bin/sh
set -eu

# POSTGRES_DB (dagster) is created by the image; these are the other three from the plan.
for database in ducklake_catalog marquez app; do
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -c "CREATE DATABASE \"$database\" OWNER \"$POSTGRES_USER\";"
done
# TODO replce with .sql file in maybe migrations dir
# OR decide on migration strategy for these databases
# could be fun...