-- depends_on: {{ source('archive', 'jar_financial_statements') }}
{{ config(
    materialized='incremental',
    alias='late_filers',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

{{ archived_csv('jar_financial_statements', 'VeluojantisPateikti*.csv.gz', 'raw_jar_financial_statements__late_filers', '_snapshot_month') }}
