-- A table, so a value that does not parse fails the build; 1.7 M vacancies a snapshot, keeping
-- no insertion order, to stay within a dbt process's memory.
{{ config(
    materialized='table',
    pre_hook=["set preserve_insertion_order = false"],
    post_hook=["reset preserve_insertion_order"]
) }}

-- Lithuania joined the euro on 2015-01-01 at this fixed rate: a few vacancies posted in early
-- 2015 still state their pay in litas.
{% set litas_per_euro = 3.4528 %}
-- An offered pay below this is per hour: no monthly pay is this low, no hourly one this high.
{% set most_hourly_pay = 50 %}

with vacancies as (
    select * from {{ ref('raw_uzt_vacancies__vacancies') }}
    -- The euro years only: older vacancies are in litas, and before Sodra's files.
    where ikelimo_data >= '2015-01-01'
),

municipalities as (
    select municipality_code, municipality from {{ ref('municipalities') }}
),

priced as (
    select
        *,
        case valiuta
            when 'EUR' then 1
            when 'LTL' then 1 / {{ litas_per_euro }}
        end as _to_euro
    from vacancies
),

typed as (
    select
        darbo_vietos_id as vacancy_id,
        statusas as status,
        ar_aktuali_siandien = '1' as is_open,
        ar_uzpildyta = '1' as is_filled,
        ikelimo_data::date as posted_on,
        galioja_nuo::date as valid_from,
        galioja_iki::date as valid_until,
        pageidaujama_darbo_pradzia::date as wanted_start_on,
        darbo_vietu_skaicius::integer as places,
        kontrakto_tipas as contract_type,
        registravimo_pagrindo_pav as registration_basis,
        registravimo_budo_pav as registration_channel,
        valiuta as currency,
        round(vid_darbo_uzmokestis::decimal(14, 2) * _to_euro, 2) as pay_from_eur,
        round(maks_darbo_uzmokestis::decimal(14, 2) * _to_euro, 2) as pay_to_eur,
        round(prelim_darbo_uzmokestis::decimal(14, 2) * _to_euro, 2) as monthly_pay_eur,
        uzmokescio_komentaras_lt as pay_comment,
        profesijos_grupes_kodas as profession_group_code,
        profesijos_grupes_pav as profession_group,
        profesijos_kodas as profession_code,
        profesijos_pareigybes_pav as profession,
        -- Natural persons are published as "Fizinis asmuo", farmers by their farm's code.
        case
            when teisines_formos_pav = 'Juridiniai asmenys' and regexp_full_match(jar_kodas, '\d{9}')
                then jar_kodas::bigint
        end as company_code,
        jar_kodas as employer_code,
        darbdavys as employer_name,
        teisines_formos_pav as employer_kind,
        teisinio_statuso_pav as employer_status,
        imones_iregistravimas::date as employer_registered_on,
        darbdavio_bustine as employer_seat,
        darbo_vietos_adresas as workplace_address,
        darbo_vietos_sav_pav as workplace_municipality,
        -- "Savanorių pr. 192, Kaunas 44151, Lietuva": the street and number first (either may
        -- be missing, and a floor or block may follow), then the settlement with an optional
        -- postcode, then the country.
        string_split(darbo_vietos_adresas, ', ') as _workplace_parts,
        -- "Vilniaus miesto sav., Vilnius, Gabijos g. 52-102, LT-06157": the municipality,
        -- eldership (sen.) and postcode may be missing; a village is in the genitive with k.
        list_filter(
            string_split(darbdavio_bustine, ', '),
            part -> not regexp_full_match(part, 'LT-\d{5}|.* (sav|sen)\.')
        ) as _seat_parts,
        _snapshot_month as snapshot_month,
        strptime(_file_last_modified, '%a, %d %b %Y %H:%M:%S GMT') as file_published_at
    from priced
),

components as (
    select
        *,
        nullif(regexp_replace(_workplace_parts[-2], '\s+\d{5}$', ''), '') as workplace_settlement,
        'LT-' || nullif(regexp_extract(_workplace_parts[-2], '\s(\d{5})$', 1), '')
            as workplace_postcode,
        case when len(_workplace_parts) >= 3 then _workplace_parts[1] end as _workplace_street,
        case when len(_seat_parts) >= 2 then _seat_parts[-2] else _seat_parts[1] end
            as _seat_settlement,
        case when len(_seat_parts) >= 2 then _seat_parts[-1] end as _seat_street
    from typed
),

parsed as (
    select
        *,
        {{ uzt_street_name('_workplace_street') }} as workplace_street,
        {{ uzt_street_type('_workplace_street') }} as workplace_street_type,
        {{ uzt_house_number('_workplace_street') }} as workplace_house_number,
        {{ uzt_street_name('_seat_street') }} as _seat_street_name,
        {{ uzt_house_number('_seat_street') }} as _seat_house_number
    from components
)

select
    parsed.vacancy_id,
    parsed.status,
    parsed.is_open,
    parsed.is_filled,
    parsed.posted_on,
    parsed.valid_from,
    parsed.valid_until,
    parsed.wanted_start_on,
    parsed.places,
    parsed.contract_type,
    parsed.registration_basis,
    parsed.registration_channel,
    parsed.currency,
    parsed.pay_from_eur,
    parsed.pay_to_eur,
    -- The ad states its pay per hour or per month; the Employment Service works out a month's
    -- pay for the hours advertised (monthly_pay_eur: 17 EUR an hour for 40 hours a week is 2,856).
    case when parsed.pay_from_eur < {{ most_hourly_pay }} then 'hour' else 'month' end
        as pay_period,
    parsed.monthly_pay_eur,
    case
        when parsed.pay_from_eur < {{ most_hourly_pay }} then parsed.monthly_pay_eur
        else parsed.pay_from_eur
    end as monthly_pay_from_eur,
    case
        when parsed.pay_from_eur < {{ most_hourly_pay }}
            then round(parsed.pay_to_eur * parsed.monthly_pay_eur / parsed.pay_from_eur, 2)
        else parsed.pay_to_eur
    end as monthly_pay_to_eur,
    parsed.pay_comment,
    parsed.profession_group_code,
    parsed.profession_group,
    parsed.profession_code,
    parsed.profession,
    parsed.company_code,
    parsed.employer_code,
    parsed.employer_name,
    parsed.employer_kind,
    parsed.employer_status,
    parsed.employer_registered_on,
    parsed.employer_seat,
    parsed.workplace_address,
    parsed.workplace_municipality,
    municipalities.municipality_code as workplace_municipality_code,
    parsed.workplace_settlement,
    parsed.workplace_street,
    parsed.workplace_street_type,
    parsed.workplace_house_number,
    parsed.workplace_postcode,
    -- Null unless both addresses name a street and a number. A seat writes a village in the
    -- genitive (Ramučių k.), the workplace in the nominative (Ramučiai), so a village's street
    -- and number alone decide.
    case
        when parsed.workplace_house_number is not null and parsed._seat_house_number is not null
            then lower(parsed.workplace_street) = lower(parsed._seat_street_name)
                and parsed.workplace_house_number = parsed._seat_house_number
                and (
                    lower(parsed.workplace_settlement) = lower(parsed._seat_settlement)
                    or ends_with(parsed._seat_settlement, ' k.')
                )
    end as workplace_is_seat,
    parsed.snapshot_month,
    parsed.file_published_at
from parsed
left join municipalities
    on parsed.workplace_municipality = municipalities.municipality
