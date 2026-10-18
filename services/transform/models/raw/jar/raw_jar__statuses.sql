-- depends_on: {{ source('archive', 'jar') }}
{{ config(
    materialized='incremental',
    alias='statuses',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

{{ archived_csv('jar', 'Statusas.csv', 'raw_jar__statuses', '_snapshot_month') }}
