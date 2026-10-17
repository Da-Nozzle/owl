-- Every snapshot's export of the register's seats (registered offices), whole, kept as
-- raw_jar.entities is: a build replaces only its snapshot's rows.
-- depends_on: {{ source('archive', 'jar') }}
{{ config(
    materialized='incremental',
    alias='seats',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

{{ archived_csv('jar', 'Buveine.csv', 'raw_jar__seats', '_snapshot_month') }}
