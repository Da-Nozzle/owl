{{ config(materialized='table') }}  -- so a value that does not parse fails the build

-- Sodra rewrites each year's file in place, so its newest download is current. Whole files are
-- picked, not rows, so an employer a revision drops is dropped here too.
with current_version as (
    select *
    from {{ ref('raw_sodra__monthly') }}
    qualify _snapshot_month = max(_snapshot_month) over (partition by parse_filename(_source_file))
),

published as (
    select
        "Draudėjo kodas (code)"::bigint as insurer_code,
        "Juridinių asmenų registro kodas (jarCode)"::bigint as company_code,
        "Pavadinimas (name)" as employer_name,
        "Savivaldybė, kurioje registruota(municipality)" as municipality,
        -- ecoActCodeStr is not used: it trims trailing zeros (960200 is 96.02).
        "Ekonominės veiklos rūšies kodas(ecoActCode)" as activity_code,
        "Ekonominės veiklos rūšies pavadinimas(ecoActName)" as activity_name,
        strptime("Mėnuo (month)", '%Y%m')::date as declared_month,
        "Apdraustųjų skaičius (numInsured)"::integer as insured_count,
        "Vidutinis darbo užmokestis (avgWage)"::decimal(14, 2) as avg_wage,
        "Valstybinio socialinio draudimo įmoka (tax)"::decimal(14, 2) as contributions,
        _snapshot_month as snapshot_month,
        strptime(_file_last_modified, '%a, %d %b %Y %H:%M:%S GMT') as file_published_at
    from current_version
)

select * replace (
    -- Exported as a number: an odd length means the leading zero was lost (11310 is 01.13.10).
    case
        when length(activity_code) % 2 = 1 then '0' || activity_code
        else activity_code
    end as activity_code
)
from published
