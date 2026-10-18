-- Every snapshot's sweep of the Places API, as the API sent it: one row per place on each page,
-- with the search that found it (its query, from the request the page's sidecar recorded, and
-- its search area, tile and page, from the file name) and the place itself in `place`, which holds the fields
-- the fetch asked for. A place found by several queries, or by a tile and its quarters, has a row
-- for each. A build replaces only the rows of its snapshot. Built from nothing (first build,
-- --full-refresh), it reads every complete snapshot in the archive.
-- depends_on: {{ source('archive', 'google_places') }}
{{ config(
    materialized='incremental',
    alias='places',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

with entries as (
    {{ archived_json_entries(
        'google_places',
        'places-*.json',
        'raw_google_places__places',
        '_snapshot_month',
        list_key='places',
        with_request=true
    ) }}
)

select
    entry ->> 'id' as id,
    -- places-<area>-<query>-<tile>-<page>.json
    regexp_extract(_source_file, '/places-([0-9]+)-[^/]+$', 1) as _search_area,
    _file_request -> 'body' ->> 'textQuery' as _search_query,
    regexp_extract(_source_file, '-(r[0-3]*)-[0-9]+\.json$', 1) as _search_tile,
    regexp_extract(_source_file, '-([0-9]+)\.json$', 1) as _search_page,
    _file_request -> 'body' -> 'locationRestriction' -> 'rectangle' as _search_rectangle,
    entry as place,
    * exclude (entry, _file_request)
from entries
