{# A table, so a value that does not parse fails the build. Its windows over a million list rows
   take memory beyond DuckDB's limit at eight threads, as the changes' JSON does. #}
{{ config(
    materialized='table',
    pre_hook=["set preserve_insertion_order = false", "set threads = 2"],
    post_hook=["reset preserve_insertion_order", "reset threads"]
) }}

{%- set columns = vmi_tax_arrears_columns() %}

with changes as (
    select * from {{ ref('stg_vmi_tax_arrears__changes') }}
),

-- In order, each with the list's newest change it holds: the list as of that snapshot's tick.
snapshots as (
    select
        snapshot_month,
        row_number() over (order by snapshot_month) as snapshot_index,
        max(changed_at) as list_changed_at
    from changes
    group by snapshot_month
),

-- Each row's changes within a snapshot, folded: its last operation, and each field's last value
-- set, wrapped so that a field set empty ({'value': null}) differs from one not set (null).
row_snapshots as (
    select
        arrears_row_id,
        snapshot_month,
        arg_max(operation, change_id) as last_operation,
        arg_max(company_code, change_id) as company_code,
        {%- for column in columns %}
        arg_max({'value': {{ column.name }}}, change_id)
            filter (where list_contains(changed_fields, '{{ column.field }}')) as {{ column.name }}{{ ',' if not loop.last }}
        {%- endfor %}
    from changes
    group by arrears_row_id, snapshot_month
),

-- The snapshots from a row's first change to its delete, or to the newest: a row the month did
-- not change still stands.
row_spans as (
    select
        row_snapshots.arrears_row_id,
        min(snapshots.snapshot_index) as first_index,
        case
            when arg_max(row_snapshots.last_operation, snapshots.snapshot_index) = 'delete'
                then max(snapshots.snapshot_index)
            else (select max(snapshot_index) from snapshots)
        end as last_index
    from row_snapshots
    inner join snapshots on row_snapshots.snapshot_month = snapshots.snapshot_month
    group by row_snapshots.arrears_row_id
),

-- Each row as it stood at each snapshot's tick: the last value set to each field up to then.
row_states as (
    select
        row_spans.arrears_row_id,
        snapshots.snapshot_month,
        snapshots.snapshot_index,
        snapshots.list_changed_at,
        last_value(row_snapshots.last_operation ignore nulls) over so_far as last_operation,
        last_value(row_snapshots.company_code ignore nulls) over so_far as company_code,
        {%- for column in columns %}
        last_value(row_snapshots.{{ column.name }} ignore nulls) over so_far as {{ column.name }}{{ ',' if not loop.last }}
        {%- endfor %}
    from row_spans
    inner join snapshots
        on snapshots.snapshot_index between row_spans.first_index and row_spans.last_index
    left join row_snapshots
        on row_spans.arrears_row_id = row_snapshots.arrears_row_id
        and snapshots.snapshot_month = row_snapshots.snapshot_month
    window so_far as (
        partition by row_spans.arrears_row_id
        order by snapshots.snapshot_index
        rows between unbounded preceding and current row
    )
),

-- A company's rows at a tick. At 2025-07's, 1,880 companies held two or three: older records the
-- next morning's clean-up (2025-07-22) deleted, so the newest record is the company's.
company_rows as (
    select
        company_code,
        snapshot_month,
        snapshot_index,
        list_changed_at,
        arrears_row_id,
        {%- for column in columns %}
        {{ column.name }}.value as {{ column.name }}{{ ',' if not loop.last }}
        {%- endfor %}
    from row_states
    -- A row whose code was published encrypted (the first loads) names no company.
    where last_operation is distinct from 'delete' and company_code is not null
    qualify row_number() over (
        partition by company_code, snapshot_index
        order by record_updated_on desc, try_cast(record_number as bigint) desc, arrears_row_id
    ) = 1
),

-- The snapshots in whose month the company was on the list at any time, its rows' spans. In
-- March 2026 VMI deleted 11,877 companies' rows and inserted them again as new records over the
-- next eleven days, across the tick: they were on the list in March, if not at its tick.
presence as (
    select distinct company_code, snapshot_index
    from row_states
    where company_code is not null
),

-- Consecutive snapshots on the list share a streak.
streaks as (
    select
        company_code,
        snapshot_index,
        snapshot_index - row_number() over (
            partition by company_code order by snapshot_index
        ) as streak
    from presence
),

listed as (
    select
        company_rows.*,
        streaks.streak,
        row_number() over (
            partition by streaks.company_code, streaks.streak order by streaks.snapshot_index
        ) as months_listed
    from streaks
    left join company_rows
        on streaks.company_code = company_rows.company_code
        and streaks.snapshot_index = company_rows.snapshot_index
    -- Counted over every snapshot of the streak, then kept where its tick found the company.
    qualify company_rows.company_code is not null
)

select
    company_code,
    snapshot_month,
    company_name,
    municipality_code,
    arrears_amount,
    overdue_amount,
    deferred_amount,
    deferred_overdue_amount,
    months_listed,
    -- The tick before within the streak that found the company on the list.
    lag(arrears_amount) over in_streak as previous_arrears_amount,
    lag(overdue_amount) over in_streak as previous_overdue_amount,
    arrears_amount - coalesce(lag(arrears_amount) over in_streak, 0) as arrears_change,
    overdue_amount - coalesce(lag(overdue_amount) over in_streak, 0) as overdue_change,
    record_number,
    record_created_on,
    record_updated_on,
    arrears_row_id,
    list_changed_at
from listed
window in_streak as (partition by company_code, streak order by snapshot_index)
