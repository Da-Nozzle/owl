-- Every snapshot's export of the register's building and land plot addresses, whole: a build
-- replaces only the rows of its snapshot, so rebuilds in any order leave the other snapshots as
-- they were. Built from nothing (first build, --full-refresh), it reads every complete snapshot
-- in the archive. A column the register adds is added here too.
-- depends_on: {{ source('archive', 'address_register') }}
{{ config(
    materialized='incremental',
    alias='buildings',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

{{ archived_csv('address_register', 'Pastatas.csv', 'raw_address_register__buildings', '_snapshot_month') }}
