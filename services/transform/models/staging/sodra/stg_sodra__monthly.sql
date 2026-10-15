-- Sodra's published headers mapped to names and types, one line per column. A renamed or
-- dropped header fails here, and so does a value that does not parse; the columns not used
-- yet are in raw_sodra.monthly. A table, so every value is cast when it is built.
{{ config(materialized='table') }}

-- Raw keeps every run's download of each yearly file. Sodra rewrites a year's file in place, so
-- the newest download of it is the current version of every month in it. Whole files are
-- picked, not rows, so an employer Sodra drops from a month in a revision is dropped here too.
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
