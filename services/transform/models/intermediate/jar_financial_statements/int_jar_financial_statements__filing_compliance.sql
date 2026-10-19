{{ config(materialized='table') }}

with years_filed as (
    select
        entity_id,
        min(fiscal_year) as first_fiscal_year_filed,
        max(fiscal_year) as last_fiscal_year_filed,
        count(*)::integer as fiscal_years_filed
    from {{ ref('int_jar_financial_statements__company_years') }}
    group by all
),

not_filed as (
    select
        entity_id,
        list(fiscal_year order by fiscal_year) as fiscal_years_not_filed,
        max(fiscal_year) as last_fiscal_year_not_filed
    from {{ ref('stg_jar_financial_statements__non_filers') }}
    group by all
),

late as (
    select entity_id from {{ ref('stg_jar_financial_statements__late_filers') }}
),

entity_ids as (
    select entity_id from years_filed
    union
    select entity_id from not_filed
    union
    select entity_id from late
),

entities as (
    select entity_id, company_code
    from {{ ref('stg_jar__entities') }}
    where snapshot_month = (select max(snapshot_month) from {{ ref('stg_jar__entities') }})
)

select
    entities.company_code,
    entity_ids.entity_id,
    years_filed.first_fiscal_year_filed,
    years_filed.last_fiscal_year_filed,
    coalesce(years_filed.fiscal_years_filed, 0) as fiscal_years_filed,
    coalesce(not_filed.fiscal_years_not_filed, []) as fiscal_years_not_filed,
    not_filed.last_fiscal_year_not_filed,
    late.entity_id is not null as is_late_filer
from entity_ids
left join years_filed on entity_ids.entity_id = years_filed.entity_id
left join not_filed on entity_ids.entity_id = not_filed.entity_id
left join late on entity_ids.entity_id = late.entity_id
left join entities on entity_ids.entity_id = entities.entity_id
