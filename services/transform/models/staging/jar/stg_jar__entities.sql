-- The register's published columns mapped to names and types, one line per column, with the legal
-- form and status each entity refers to by _id. A value that does not parse fails here; the columns
-- not used yet are in raw_jar.entities. A table, so every value is cast when it is built.
{{ config(materialized='table') }}

-- Every snapshot is the whole register as fetched then, so each is kept: the leads of a month read
-- the register as of that month. A reference is resolved in the classifiers of the entity's own
-- snapshot, so a classifier fetched by several snapshots matches each entity once.
with entities as (
    select * from {{ ref('raw_jar__entities') }}
),

legal_forms as (
    select _snapshot_month, _id, kodas, pavadinimas from {{ ref('raw_jar__legal_forms') }}
),

statuses as (
    select _snapshot_month, _id, kodas, pavadinimas from {{ ref('raw_jar__statuses') }}
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
    entities._snapshot_month as snapshot_month,
    strptime(entities._file_last_modified, '%a, %d %b %Y %H:%M:%S GMT') as file_published_at
from entities
left join legal_forms
    on entities._snapshot_month = legal_forms._snapshot_month
    and entities."forma._id" = legal_forms._id
left join statuses
    on entities._snapshot_month = statuses._snapshot_month
    and entities."statusas._id" = statuses._id
