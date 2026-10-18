-- depends_on: {{ source('archive', 'address_register') }}
{{ config(
    materialized='incremental',
    alias='municipalities',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

{{ archived_csv('address_register', 'Savivaldybe.csv', 'raw_address_register__municipalities', '_snapshot_month') }}
