-- The register's addresses mapped to names and types, one line per column: every building's or
-- land plot's address, and every premises' address within its building, with the street,
-- settlement and municipality each is in. A value that does not parse fails here; the columns not
-- used yet are in raw_address_register. A table, so every value is cast when it is built.
{{ config(materialized='table') }}

-- Every snapshot is the whole register as fetched then, so each is kept, as JAR's are: a seat in
-- JAR's snapshot of a month is resolved in this register's snapshot of the same month. A
-- reference is resolved in the datasets of the address's own snapshot, so a dataset fetched by
-- several snapshots matches each address once.
with buildings as (
    select * from {{ ref('raw_address_register__buildings') }}
),

premises as (
    select * from {{ ref('raw_address_register__premises') }}
),

streets as (
    select _snapshot_month, _id, gat_kodas, pavadinimas, tipo_santrumpa
    from {{ ref('raw_address_register__streets') }}
),

settlements as (
    select _snapshot_month, _id, gyv_kodas, pavadinimas, tipo_santrumpa
    from {{ ref('raw_address_register__settlements') }}
),

municipalities as (
    select _snapshot_month, _id, sav_kodas, pavadinimas
    from {{ ref('raw_address_register__municipalities') }}
),

building_addresses as (
    select
        buildings._snapshot_month,
        buildings._id as building_id,
        buildings."aob_kodas._id" as address_id,
        municipalities.sav_kodas::integer as municipality_code,
        municipalities.pavadinimas as municipality,
        settlements.gyv_kodas::integer as settlement_code,
        settlements.pavadinimas as settlement,
        settlements.tipo_santrumpa as settlement_type,
        streets.gat_kodas::integer as street_code,
        streets.pavadinimas as street,
        streets.tipo_santrumpa as street_type,
        buildings.nr as house_number,
        buildings.korpuso_nr as building_block,
        buildings.pasto_kodas as postcode,
        buildings.aob_nuo::date as registered_on,
        buildings._file_last_modified
    from buildings
    left join municipalities
        on buildings._snapshot_month = municipalities._snapshot_month
        and buildings."savivaldybe._id" = municipalities._id
    left join settlements
        on buildings._snapshot_month = settlements._snapshot_month
        and buildings."gyvenamoji_vietove._id" = settlements._id
    left join streets
        on buildings._snapshot_month = streets._snapshot_month
        and buildings."gatve._id" = streets._id
),

-- A premises' address is its building's with the premises' number.
premises_addresses as (
    select
        premises."aob_kodas._id" as address_id,
        true as is_premises,
        building_addresses.address_id as building_address_id,
        building_addresses.municipality_code,
        building_addresses.municipality,
        building_addresses.settlement_code,
        building_addresses.settlement,
        building_addresses.settlement_type,
        building_addresses.street_code,
        building_addresses.street,
        building_addresses.street_type,
        building_addresses.house_number,
        building_addresses.building_block,
        premises.patalpos_nr as premises_number,
        building_addresses.postcode,
        premises.pat_nuo::date as registered_on,
        premises._snapshot_month as snapshot_month,
        premises._file_last_modified
    from premises
    left join building_addresses
        on premises._snapshot_month = building_addresses._snapshot_month
        and premises."pastatas._id" = building_addresses.building_id
),

addresses as (
    select
        address_id,
        false as is_premises,
        address_id as building_address_id,
        municipality_code,
        municipality,
        settlement_code,
        settlement,
        settlement_type,
        street_code,
        street,
        street_type,
        house_number,
        building_block,
        null::varchar as premises_number,
        postcode,
        registered_on,
        _snapshot_month as snapshot_month,
        _file_last_modified
    from building_addresses

    union all

    select * from premises_addresses
)

select
    * exclude (_file_last_modified),
    strptime(_file_last_modified, '%a, %d %b %Y %H:%M:%S GMT') as file_published_at
from addresses
