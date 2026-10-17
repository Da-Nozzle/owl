-- The taxpayer register as it stands after the newest change fetched: the newest export, with every
-- change made after it applied in changelog order. The register's published columns mapped to
-- names and types, one line per column (macros/vmi.sql), and each activity resolved in the EVRK 2
-- classifier exported with it. A table, so every value is cast when it is built.
{{ config(materialized='table') }}

{%- set columns = vmi_register_columns() %}

with export as (
    select *
    from {{ ref('raw_vmi_monthly_register__register') }}
    where _snapshot_month = (select max(_snapshot_month) from {{ ref('raw_vmi_monthly_register__register') }})
),

-- Where in the changelog the export stands: it holds every change up to its last change.
last_change as (
    select _cid::bigint as change_id
    from {{ ref('raw_vmi_monthly_register__last_change') }}
    where _snapshot_month = (select max(_snapshot_month) from export)
),

-- Each row's latest value of every field a change after the export set, as JSON: absent when no
-- such change set the field, JSON null when the latest one emptied it. An insert sets them all.
changed_rows as (
    select
        register_row_id,
        arg_max(operation, change_id) as last_operation,
        max(changed_at) as changed_at,
        {%- for column in columns %}
        arg_max(change -> '$.{{ column.published.split(".")[0] }}', change_id)
            as "{{ column.published.split(".")[0] }}"{{ ',' if not loop.last }}
        {%- endfor %}
    from {{ ref('stg_vmi__changes') }}
    where change_id > (select change_id from last_change)
    group by register_row_id
),

current_rows as (
    select
        coalesce(changed_rows.register_row_id, export._id) as register_row_id,
        {%- for column in columns %}
        {%- set field = column.published.split(".")[0] %}
        {%- set value = '$._id' if '.' in column.published else '$' %}
        case
            when changed_rows."{{ field }}" is not null then changed_rows."{{ field }}" ->> '{{ value }}'
            else export."{{ column.published }}"
        end as "{{ column.published }}",
        {%- endfor %}
        changed_rows.changed_at,
        export._snapshot_month as export_month
    from export
    full join changed_rows on export._id = changed_rows.register_row_id
    where changed_rows.last_operation is distinct from 'delete'
),

activities as (
    select _id, kodas, pavadinimas_lt
    from {{ ref('raw_vmi_monthly_register__activities') }}
    where _snapshot_month = (select max(_snapshot_month) from export)
)

select
    current_rows.register_row_id,
    {%- for column in columns %}
    {{ vmi_typed('current_rows."' ~ column.published ~ '"', column) }} as {{ column.name }},
    {%- endfor %}
    activities.kodas as activity_code,
    activities.pavadinimas_lt as activity_name,
    current_rows.changed_at,
    current_rows.export_month
from current_rows
left join activities on current_rows."ekonomine_veikla._id" = activities._id
