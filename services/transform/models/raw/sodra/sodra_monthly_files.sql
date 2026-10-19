-- depends_on: {{ source('archive', 'sodra') }}
{{ config(materialized='ephemeral') }}

{% set snapshots = archived_snapshots('raw_sodra__monthly', 'sodra') %}
{% set files = listed_archived_files('sodra', 'monthly-*.csv.zip', snapshots) %}

with published as (
    select *
    from read_csv(
        [
            {%- for zipped in files %}
            'zip://{{ zipped }}/*.csv'{{ ',' if not loop.last }}
            {%- endfor %}
        ],
        delim = ';',
        quote = '"',
        header = true,
        all_varchar = true,  -- types are given once, in stg_sodra__monthly
        union_by_name = true,  -- the 2025 file has no ecoActCodeStr; its rows get null
        filename = '_source_file'
    )
),

-- Sidecars written before size and sha256 were recorded have them null.
fetches as (
    select
        regexp_replace(_meta_file, '\.meta\.json$', '') as archived_file,
        url,
        fetched_at,
        last_modified,
        sha256
    from read_json(
        {{ sidecars_of(files) }},
        columns = {
            url: 'varchar',
            fetched_at: 'varchar',
            last_modified: 'varchar',
            size: 'varchar',
            sha256: 'varchar'
        },
        filename = '_meta_file'
    )
)

select
    published.*,
    regexp_extract(published._source_file, '/(\d{4}-\d{2})/[^/]+\.zip/', 1) as _snapshot_month,
    fetches.url as _file_url,
    fetches.fetched_at as _file_fetched_at,
    fetches.last_modified as _file_last_modified,
    fetches.sha256 as _file_sha256
from published
left join fetches
    on regexp_extract(published._source_file, '^zip://(.+\.zip)/', 1) = fetches.archived_file
