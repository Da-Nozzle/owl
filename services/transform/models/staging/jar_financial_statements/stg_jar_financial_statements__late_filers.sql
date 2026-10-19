{{ config(materialized='table') }}

{#- The newest snapshot only. The dataset names no year (veluojantis_iki and ja_reg_data are empty
    on every row): it is the list as of its publication. #}

with late_filers as (
    select *
    from {{ ref('raw_jar_financial_statements__late_filers') }}
    where _snapshot_month = (
        select max(_snapshot_month) from {{ ref('raw_jar_financial_statements__late_filers') }}
    )
)

select
    "juridinis_asmuo._id" as entity_id,
    count(*)::integer as published_copies,
    any_value(_snapshot_month) as snapshot_month,
    strptime(any_value(_file_last_modified), '%a, %d %b %Y %H:%M:%S GMT') as file_published_at
from late_filers
group by all
