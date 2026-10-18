-- depends_on: {{ source('archive', 'address_register') }}
{{ config(
    materialized='incremental',
    alias='premises',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

{{ archived_csv('address_register', 'Patalpa.csv', 'raw_address_register__premises', '_snapshot_month') }}
