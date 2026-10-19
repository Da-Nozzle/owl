{# Every row of the archived CSV `name` (a glob: a paged export's pages), all text, with its
   file, snapshot and the fetch its sidecar recorded, from the files the snapshots' manifests
   list. The dialect is get.data.gov.lt's export. #}
{% macro archived_csv(source, name, model_name, snapshot_column) %}
    {%- set snapshots = archived_snapshots(model_name, source) -%}
    {%- set files = listed_archived_files(source, name, snapshots) -%}
    {%- if not files -%}
        {{ exceptions.raise_compiler_error(
            "The manifests of " ~ source ~ "'s snapshots " ~ snapshots
            ~ " list no file matching " ~ name
        ) }}
    {%- endif -%}
with published as (
    select *
    from read_csv(
        {{ files }},
        delim = ',',
        quote = '"',
        escape = '"',
        header = true,
        all_varchar = true,  -- types are given once, in staging
        union_by_name = true,  -- a column added in a later snapshot is null in earlier ones
        filename = '_source_file'
    )
),

fetches as (
    select _meta_file, url, fetched_at, last_modified, sha256
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
    regexp_extract(published._source_file, '/([^/]+)/[^/]+$', 1) as {{ snapshot_column }},
    fetches.url as _file_url,
    fetches.fetched_at as _file_fetched_at,
    fetches.last_modified as _file_last_modified,
    fetches.sha256 as _file_sha256
from published
left join fetches
    on published._source_file || '.meta.json' = fetches._meta_file
{% endmacro %}

{# One row per entry of each listed JSON page's `list_key` array ({"_data": [...]} from
   get.data.gov.lt, {"places": [...]} from the Places API), as JSON in `entry`, with its file,
   snapshot and sidecar fetch; with `with_request`, also the request the page answered. A page
   may be up to `maximum_object_size` bytes (64 MB; a page of 10,000 changes is about 3 MB), and
   each thread reading one holds twice that: a source read many pages at once keeps it near its
   pages' size, or eight threads ask for 1 GB. #}
{% macro archived_json_entries(source, pattern, model_name, snapshot_column, list_key='_data', with_request=false, maximum_object_size=67108864) %}
    {%- set snapshots = archived_snapshots(model_name, source) -%}
    {%- set files = listed_archived_files(source, pattern, snapshots) -%}
    {%- if files %}
with published as (
    select unnest({{ list_key }}) as entry, _source_file
    from read_json(
        {{ files }},
        columns = { {{- list_key }}: 'json[]'},
        format = 'unstructured',
        maximum_object_size = {{ maximum_object_size }},
        filename = '_source_file'
    )
),

fetches as (
    select _meta_file, url, fetched_at, last_modified, sha256{% if with_request %}, request{% endif %}
    from read_json(
        {{ sidecars_of(files) }},
        columns = {
            url: 'varchar',
            fetched_at: 'varchar',
            last_modified: 'varchar',
            size: 'varchar',
            sha256: 'varchar'{% if with_request %},
            request: 'json'{% endif %}
        },
        filename = '_meta_file'
    )
)

select
    published.entry,
    published._source_file,
    regexp_extract(published._source_file, '/([^/]+)/[^/]+$', 1) as {{ snapshot_column }},
    fetches.url as _file_url,
    fetches.fetched_at as _file_fetched_at,
    fetches.last_modified as _file_last_modified,
    fetches.sha256 as _file_sha256{% if with_request %},
    fetches.request as _file_request{% endif %}
from published
left join fetches
    on published._source_file || '.meta.json' = fetches._meta_file
    {%- else %}
    {#- No page to read (a day with no changes has none), and read_json takes no empty list. #}
select
    null::json as entry,
    null::varchar as _source_file,
    null::varchar as {{ snapshot_column }},
    null::varchar as _file_url,
    null::varchar as _file_fetched_at,
    null::varchar as _file_last_modified,
    null::varchar as _file_sha256{% if with_request %},
    null::json as _file_request{% endif %}
where false
    {%- endif %}
{% endmacro %}
