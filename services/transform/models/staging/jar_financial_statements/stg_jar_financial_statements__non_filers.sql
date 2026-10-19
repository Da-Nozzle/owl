{{ config(materialized='table') }}

{#- The newest snapshot only, as the lines. An entity is listed again for a year whenever its
    form or status changed between loads, so a year is kept once. #}

with non_filers as (
    select *
    from {{ ref('raw_jar_financial_statements__non_filers') }}
    where _snapshot_month = (
        select max(_snapshot_month) from {{ ref('raw_jar_financial_statements__non_filers') }}
    )
)

select
    "juridinis_asmuo._id" as entity_id,
    nepateikta_iki::integer as fiscal_year,
    count(*)::integer as published_copies,
    any_value(_snapshot_month) as snapshot_month,
    strptime(any_value(_file_last_modified), '%a, %d %b %Y %H:%M:%S GMT') as file_published_at
from non_filers
group by all
