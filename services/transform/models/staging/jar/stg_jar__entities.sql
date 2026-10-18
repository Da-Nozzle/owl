{{ config(materialized='table') }}  -- so a value that does not parse fails the build

with entities as (
    select * from {{ ref('raw_jar__entities') }}
),

legal_forms as (
    select _snapshot_month, _id, kodas, pavadinimas from {{ ref('raw_jar__legal_forms') }}
),

statuses as (
    select _snapshot_month, _id, kodas, pavadinimas from {{ ref('raw_jar__statuses') }}
),

seats as (
    select _snapshot_month, "juridinis_asmuo._id", "adresas._id", adresas_nuo
    from {{ ref('raw_jar__seats') }}
)

select
    entities.ja_kodas::bigint as company_code,
    entities.ja_pavadinimas as company_name,
    legal_forms.kodas::integer as legal_form_code,
    legal_forms.pavadinimas as legal_form,
    statuses.kodas::integer as status_code,
    statuses.pavadinimas as status,
    entities.reg_data::date as registered_on,
    entities.isreg_data::date as deregistered_on,
    entities.stat_data::date as status_since,
    seats."adresas._id" as registered_address_id,
    seats.adresas_nuo::date as registered_address_since,
    entities._snapshot_month as snapshot_month,
    strptime(entities._file_last_modified, '%a, %d %b %Y %H:%M:%S GMT') as file_published_at
from entities
-- References resolve in the entity's own snapshot: every snapshot holds each dataset whole.
left join legal_forms
    on entities._snapshot_month = legal_forms._snapshot_month
    and entities."forma._id" = legal_forms._id
left join statuses
    on entities._snapshot_month = statuses._snapshot_month
    and entities."statusas._id" = statuses._id
left join seats
    on entities._snapshot_month = seats._snapshot_month
    and entities._id = seats."juridinis_asmuo._id"
