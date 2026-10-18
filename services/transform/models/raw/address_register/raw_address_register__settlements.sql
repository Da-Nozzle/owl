-- depends_on: {{ source('archive', 'address_register') }}
{{ config(
    materialized='incremental',
    alias='settlements',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

{{ archived_csv('address_register', 'GyvenamojiVietove.csv', 'raw_address_register__settlements', '_snapshot_month') }}
