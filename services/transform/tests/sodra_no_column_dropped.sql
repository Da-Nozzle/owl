-- Sodra may add a column to a year's file but not drop one: read by name, a dropped column would
-- turn into nulls for the new months. Fails on a column older files fill and the newest leaves empty.
with filled as (
    unpivot (
        select _source_file, count(columns(c -> not starts_with(c, '_')))  -- _ columns are ours
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
