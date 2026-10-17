-- Every snapshot's export of the register's legal form classifier, whole, kept as raw_jar.entities
-- is: a build replaces only its snapshot's rows.
-- depends_on: {{ source('archive', 'jar') }}
{{ config(
    materialized='incremental',
    alias='legal_forms',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

{{ archived_csv('jar', 'Forma.csv', 'raw_jar__legal_forms', '_snapshot_month') }}
