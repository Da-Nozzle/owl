{{ config(materialized='table') }}

with municipalities as (
    select * from {{ ref('municipalities') }}
),

scope as (
    select * from {{ ref('google_search_scope') }}
),

-- Sodra codes in EVRK 2.1 since 2025; the few rows it still codes in EVRK 2 are not counted.
categories as (
    select category, code_prefix
    from {{ ref('activity_categories') }}
    where classification = 'EVRK 2.1'
),

latest as (
    select *
    from {{ ref('stg_sodra__monthly') }}
    where declared_month = (select max(declared_month) from {{ ref('stg_sodra__monthly') }})
),

employers as (
    select
        latest.municipality,
        categories.category,
        count(distinct latest.insurer_code) as employers
    from latest
    inner join categories on starts_with(latest.activity_code, categories.code_prefix)
    group by all
),

counted as (
    select
        municipalities.municipality_code,
        municipalities.municipality,
        coalesce(sum(employers.employers), 0)::integer as target_employers,
        coalesce(
            map_from_entries(
                list((employers.category, employers.employers::integer) order by employers.category)
                    filter (where employers.category is not null)
            ),
            map()
        ) as employers_by_category,
        scope.search_mode,
        municipalities.south,
        municipalities.west,
        municipalities.north,
        municipalities.east
    from municipalities
    left join employers on municipalities.sodra_municipality = employers.municipality
    left join scope on municipalities.municipality_code = scope.municipality_code
    group by all
)

select
    *,
    rank() over (order by target_employers desc) as target_employers_rank,
    (select max(declared_month) from latest) as declared_month
from counted
