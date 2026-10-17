-- One row per change the taxpayer register's changelog holds for the days fetched: when it was
-- made, how, to which register row, the fields it set, and the company the row belongs to. A
-- table, so every value is cast when it is built.
{{ config(materialized='table') }}

with changes as (
    select * from {{ ref('raw_vmi_daily_changes__changes') }}
),

-- A patch or a delete names only its register row. The row's company is in the newest export that
-- holds the row, or in the row's insert among the changes; a row deleted before every export,
-- whose insert is older than the first day fetched, has none.
exported_rows as (
    select _id, ja_kodas
    from {{ ref('raw_vmi_monthly_register__register') }}
    qualify row_number() over (partition by _id order by _snapshot_month desc) = 1
),

inserted_rows as (
    select _id, change ->> 'ja_kodas' as ja_kodas
    from changes
    where _op = 'insert'
    qualify row_number() over (partition by _id order by _cid::bigint desc) = 1
)

select
    changes._cid::bigint as change_id,
    changes._created::timestamp as changed_at,
    changes._op as operation,
    changes._id as register_row_id,
    coalesce(
        changes.change ->> 'ja_kodas', exported_rows.ja_kodas, inserted_rows.ja_kodas
    )::bigint as company_code,
    list_filter(json_keys(changes.change), field -> not starts_with(field, '_')) as changed_fields,
    changes.change,
    changes._txn as transaction_id,
    changes._snapshot_day as snapshot_day
from changes
left join exported_rows on changes._id = exported_rows._id
left join inserted_rows on changes._id = inserted_rows._id
