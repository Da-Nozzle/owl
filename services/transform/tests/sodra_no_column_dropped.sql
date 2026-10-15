-- Sodra may add a column to a year's file but not drop one: a column that an older file fills
-- must have values in the newest file. Reading by name would otherwise turn a dropped column
-- into nulls for the new months. Returns each column that went empty.
with filled as (
    unpivot (
        select _source_file, count(columns(* exclude (_source_file, _snapshot_month)))
        from {{ ref('sodra_monthly_files') }}
        group by _source_file
    )
    on columns(* exclude (_source_file))
    into name column_name value filled_values
)

select column_name
from filled
group by column_name
having max(filled_values) > 0
    and max(filled_values) filter (
        where _source_file = (select max(_source_file) from filled)
    ) = 0
