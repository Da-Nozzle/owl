-- depends_on: {{ source('archive', 'vmi_monthly_register') }}
{{ config(
    materialized='incremental',
    alias='activities',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

{{ archived_csv('vmi_monthly_register', 'EkonominesVeiklosRusis.csv', 'raw_vmi_monthly_register__activities', '_snapshot_month') }}
