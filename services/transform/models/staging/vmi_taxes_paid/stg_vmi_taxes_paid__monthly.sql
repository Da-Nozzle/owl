{{ config(
    materialized='table',
    pre_hook=["set preserve_insertion_order = false"],
    post_hook=["reset preserve_insertion_order"]
) }}  -- a table, so a value that does not parse fails the build

{%- set fields = ['id', 'mm_kodas', 'pavadinimas', 'tipas', 'savivaldybe', 'metai', 'menuo', 'suma', 'atnaujinta'] %}

-- Each row a load inserted (or upserted), with every field. A delete only means that a later load
-- replaced the row, so it is not read; a row inserted before the first month fetched is known
-- only by its delete, and is left out.
with inserts as (
    select
        _id as row_id,
        _created::timestamp as loaded_at,
        {%- for field in fields %}
        change -> '$.{{ field }}' as "{{ field }}"{{ ',' if not loop.last }}
        {%- endfor %}
    from {{ ref('raw_vmi_taxes_paid__changes') }}
    -- Each row is inserted once (row_id's unique test): a load writes new rows.
    where _op in ('insert', 'upsert')
),

-- What patches set after a row's insert, field by field: SQL null when none set the field, JSON
-- null when the latest emptied it. Few rows are patched (to point mm_kodas at a new JAR entity).
patches as (
    select
        _id as row_id,
        {%- for field in fields %}
        arg_max(change -> '$.{{ field }}', _cid::bigint) as "{{ field }}"{{ ',' if not loop.last }}
        {%- endfor %}
    from {{ ref('raw_vmi_taxes_paid__changes') }}
    where _op = 'patch'
    group by _id
),

loaded_rows as (
    select
        inserts.row_id,
        inserts.loaded_at,
        {%- for field in fields %}
        coalesce(patches."{{ field }}", inserts."{{ field }}") as "{{ field }}"{{ ',' if not loop.last }}
        {%- endfor %}
    from inserts
    left join patches on inserts.row_id = patches.row_id
),

typed as (
    select
        row_id,
        "id" ->> '$' as record_id,
        "mm_kodas" ->> '$._id' as jar_entity_id,
        "pavadinimas" ->> '$' as taxpayer_name,
        "tipas" ->> '$' as legal_form,
        "savivaldybe" ->> '$._id' as municipality_id,
        ("metai" ->> '$')::integer as tax_year,
        ("menuo" ->> '$')::integer as through_month,
        ("suma" ->> '$')::decimal(18, 2) as year_to_date_eur,
        ("atnaujinta" ->> '$')::date as published_on,
        loaded_at
    from loaded_rows
),

-- An entity's _id stands for its code in every snapshot of the register.
jar_entities as (
    select _id as entity_id, arg_max(ja_kodas, _snapshot_month)::bigint as company_code
    from {{ ref('raw_jar__entities') }}
    group by _id
),

-- Names held by one company only: a name two companies share matches neither.
jar_names as (
    select {{ normalized_name('company_name') }} as name, min(company_code) as company_code
    from {{ ref('stg_jar__entities') }}
    where snapshot_month = (select max(snapshot_month) from {{ ref('stg_jar__entities') }})
    group by all
    having count(distinct company_code) = 1
),

register_names as (
    select {{ normalized_name('taxpayer_name') }} as name, min(company_code) as company_code
    from {{ ref('stg_vmi__current_register') }}
    group by all
    having count(distinct company_code) = 1
),

municipalities as (
    select _id, sav_kodas::integer as municipality_code, pavadinimas as municipality
    from {{ ref('raw_address_register__municipalities') }}
    where _snapshot_month = (
        select max(_snapshot_month) from {{ ref('raw_address_register__municipalities') }}
    )
),

resolved as (
    select
        typed.*,
        jar_entities.company_code as referenced_company_code,
        -- Matched by name only when no entity the register holds is referred to.
        case
            when jar_entities.company_code is null then {{ normalized_name('typed.taxpayer_name') }}
        end as unresolved_name
    from typed
    left join jar_entities on typed.jar_entity_id = jar_entities.entity_id
),

-- Each unresolved name matched exactly to a company: JAR's names first, then the VMI register's
-- (VMI's own, which hold companies newer than JAR's publication).
name_matches as (
    select
        unresolved.name,
        coalesce(jar_names.company_code, register_names.company_code) as company_code,
        case when jar_names.company_code is not null then 'jar_name' else 'vmi_register_name' end
            as match_method
    from (select distinct unresolved_name as name from resolved) as unresolved
    left join jar_names on unresolved.name = jar_names.name
    left join register_names on unresolved.name = register_names.name
    where coalesce(jar_names.company_code, register_names.company_code) is not null
),

matched as (
    select
        resolved.* exclude (referenced_company_code, unresolved_name),
        coalesce(resolved.referenced_company_code, name_matches.company_code) as company_code,
        case
            when resolved.referenced_company_code is not null then 'jar_reference'
            else name_matches.match_method
        end as match_method,
        municipalities.municipality_code,
        municipalities.municipality
    from resolved
    left join name_matches on resolved.unresolved_name = name_matches.name
    left join municipalities on resolved.municipality_id = municipalities._id
)

select
    company_code,
    match_method,
    taxpayer_name,
    legal_form,
    municipality_code,
    municipality,
    tax_year,
    through_month,
    year_to_date_eur,
    -- Paid in the months since the company's previous month published that year, or since
    -- January; a payer matched to no company cannot be followed from load to load.
    case when company_code is not null then
        year_to_date_eur - coalesce(lag(year_to_date_eur) over company_year, 0)
    end as paid_eur,
    case when company_code is not null then
        coalesce(lag(through_month) over company_year + 1, 1)
    end as paid_from_month,
    published_on,
    loaded_at,
    record_id,
    row_id
from matched
window company_year as (partition by company_code, tax_year order by through_month)
