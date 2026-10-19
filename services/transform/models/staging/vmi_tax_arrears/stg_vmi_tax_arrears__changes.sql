{# A table, so a value that does not parse fails the build. #}
{{ config(
    materialized='table',
    pre_hook=["set preserve_insertion_order = false"],
    post_hook=["reset preserve_insertion_order"]
) }}

{#- 3.1 M changes, 1.1 GB of JSON: read once, and with raw's settings (see its model), as JSON
    parsing holds memory beyond DuckDB's limit. #}

{%- set columns = vmi_tax_arrears_columns() %}

with changes as (
    select
        _cid::bigint as change_id,
        _created::timestamp as changed_at,
        _op as operation,
        _id as arrears_row_id,
        change ->> 'ja_kodas' as published_code,
        list_filter(json_keys(change), field -> not starts_with(field, '_')) as changed_fields,
        {%- for column in columns %}
        (change ->> '$.{{ column.published }}')::{{ column.type }} as {{ column.name }},
        {%- endfor %}
        _txn as transaction_id,
        _snapshot_month as snapshot_month
    from {{ ref('raw_vmi_tax_arrears__changes') }}
),

-- A patch, a delete or an upsert of a row the list holds names only its list row: its company is
-- in the row's insert (or the upsert that added it), which the changelog holds from its first
-- load on.
row_companies as (
    select arrears_row_id, arg_max(published_code, change_id) as published_code
    from changes
    where published_code is not null
    group by arrears_row_id
)

select
    changes.change_id,
    changes.changed_at,
    changes.operation,
    changes.arrears_row_id,
    -- The first loads (2024-11-18 to 11-26) held 11 rows whose code was published encrypted or
    -- hashed: they have no company.
    case
        when regexp_full_match(coalesce(changes.published_code, row_companies.published_code), '[0-9]+')
            then coalesce(changes.published_code, row_companies.published_code)::bigint
    end as company_code,
    changes.changed_fields,
    {%- for column in columns %}
    changes.{{ column.name }},
    {%- endfor %}
    changes.transaction_id,
    changes.snapshot_month
from changes
left join row_companies on changes.arrears_row_id = row_companies.arrears_row_id
