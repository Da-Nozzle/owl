-- Every company in the country by month (about 4.7 M rows), keeping no insertion order,
-- to stay within a dbt process's memory.
{{ config(
    materialized='table',
    pre_hook=["set preserve_insertion_order = false"],
    post_hook=["reset preserve_insertion_order"]
) }}

-- A company with several insurer codes in a month (rare) is one employer: its headcount summed,
-- its activity and municipality those of its biggest code.
with sodra as (
    select
        company_code,
        declared_month as month,
        sum(insured_count) as insured_count,
        arg_max(activity_code, insured_count) as activity_code,
        arg_max(municipality, insured_count) as municipality,
        max(file_published_at)::date as sodra_as_of
    from {{ ref('stg_sodra__monthly') }}
    where company_code is not null
    group by all
),

-- The profile covers Sodra's months: every risk model is judged on a month Sodra has declared.
months as (
    select unnest(generate_series(min(month), max(month), interval 1 month))::date as month
    from sodra
),

-- The register as it stands: one snapshot holds every entity, deregistered ones included.
jar as (
    select *
    from {{ ref('stg_jar__entities') }}
    where snapshot_month = (select max(snapshot_month) from {{ ref('stg_jar__entities') }})
),

addresses as (
    select address_id, municipality_code
    from {{ ref('stg_address_register__addresses') }}
    where snapshot_month = (
        select max(snapshot_month) from {{ ref('stg_address_register__addresses') }}
    )
),

-- Each entity in every month it was registered on some day of, and Sodra's employers JAR does
-- not hold.
spine as (
    select jar.company_code, months.month
    from jar
    inner join months
        on jar.registered_on < months.month + interval 1 month
        and (jar.deregistered_on is null or jar.deregistered_on >= months.month)
    union
    select company_code, month from sodra
),

-- The VMI register's main activities (EVRK 2), for the months Sodra gives none: VMI ended most
-- of them on 2024-12-31, at the switch to EVRK 2.1, so the latest one started is taken, in force
-- or not.
vmi_main_activities as (
    select company_code, activity_code, activity_from
    from {{ ref('stg_vmi__current_register') }}
    where is_main_activity
        and activity_annulled_on is null
        and activity_code is not null
        and activity_from is not null
),

with_sodra as (
    select
        spine.company_code,
        spine.month,
        last_day(spine.month) as month_end,
        sodra.insured_count,
        -- Sodra's latest activity and municipality up to the month, through months it declares
        -- nothing in.
        last_value(sodra.activity_code ignore nulls) over to_date as sodra_activity_code,
        last_value(sodra.municipality ignore nulls) over to_date as sodra_municipality,
        sodra.sodra_as_of,
        count(sodra.month) over to_date as sodra_months_to_date
    from spine
    left join sodra on spine.company_code = sodra.company_code and spine.month = sodra.month
    window to_date as (
        partition by spine.company_code order by spine.month
        rows between unbounded preceding and current row
    )
),

with_activity as (
    select
        with_sodra.*,
        vmi_main_activities.activity_code as vmi_activity_code
    from with_sodra
    asof left join vmi_main_activities
        on with_sodra.company_code = vmi_main_activities.company_code
        and with_sodra.month_end >= vmi_main_activities.activity_from
),

profiled as (
    select
        with_activity.company_code,
        with_activity.month,
        jar.company_name,
        jar.legal_form_code,
        jar.legal_form,
        jar.registered_on,
        jar.deregistered_on,
        date_diff('month', jar.registered_on, with_activity.month_end) as age_months,
        coalesce(
            jar.registered_on <= with_activity.month_end
            and (jar.deregistered_on is null or jar.deregistered_on > with_activity.month_end),
            false
        ) as is_registered,
        -- The register publishes only the current status and since when: before that, unknown.
        case when jar.status_since <= with_activity.month_end then jar.status_code end
            as jar_status_code,
        case when jar.status_since <= with_activity.month_end then jar.status end as jar_status,
        coalesce(
            jar.status_since <= with_activity.month_end
            and jar.status_code in ({{ var('risk_wind_down_status_codes') | join(', ') }}),
            false
        ) as is_winding_down,
        coalesce(with_activity.sodra_activity_code, with_activity.vmi_activity_code)
            as activity_code,
        case
            when with_activity.sodra_activity_code is not null then 'EVRK 2.1'
            when with_activity.vmi_activity_code is not null then 'EVRK 2'
        end as activity_classification,
        case
            when with_activity.sodra_activity_code is not null then 'sodra'
            when with_activity.vmi_activity_code is not null then 'vmi_register'
        end as activity_source,
        coalesce(sodra_municipalities.municipality_code, addresses.municipality_code)
            as municipality_code,
        case
            when sodra_municipalities.municipality_code is not null then 'sodra'
            when addresses.municipality_code is not null then 'jar_seat'
        end as municipality_source,
        with_activity.insured_count,
        with_activity.sodra_months_to_date,
        jar.file_published_at::date as jar_as_of,
        with_activity.sodra_as_of
    from with_activity
    left join jar on with_activity.company_code = jar.company_code
    left join {{ ref('municipalities') }} as sodra_municipalities
        on with_activity.sodra_municipality = sodra_municipalities.sodra_municipality
    left join addresses on jar.registered_address_id = addresses.address_id
)

select
    profiled.* exclude (
        municipality_source, insured_count, sodra_months_to_date, jar_as_of, sodra_as_of
    ),
    -- The class (four digits, 56.11) when the code is at least that fine: the peers' level.
    case when length(profiled.activity_code) >= 4 then left(profiled.activity_code, 4) end
        as activity_class,
    categories.category as leads_category,
    municipalities.municipality,
    profiled.municipality_source,
    profiled.insured_count,
    profiled.sodra_months_to_date,
    profiled.jar_as_of,
    profiled.sodra_as_of
from profiled
left join {{ ref('activity_categories') }} as categories
    on profiled.activity_classification = categories.classification
    and starts_with(profiled.activity_code, categories.code_prefix)
left join {{ ref('municipalities') }} as municipalities
    on profiled.municipality_code = municipalities.municipality_code
