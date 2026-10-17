{# The raw archive's root: <archive>/<source>/<snapshot>/<name>. #}
{% macro archive_root() -%}
    {{ env_var('LEADS_ARCHIVE_PATH').rstrip('/') }}
{%- endmacro %}

{# A file name (or glob) in each of the snapshots given, as a list of paths, which read_csv and
   read_json take as they are. #}
{% macro archived_files(source, name, snapshots) -%}
    {%- set paths = [] -%}
    {%- for key in snapshots -%}
        {%- do paths.append(archive_root() ~ '/' ~ source ~ '/' ~ key ~ '/' ~ name) -%}
    {%- endfor -%}
    {{- return(paths) -}}
{%- endmacro %}

{# The archived CSV `name` of the snapshots a build of `model_name` reads (archived_snapshots),
   whole and as published: every row, every column as text under its published header, plus the
   file it was read from, its snapshot (in `snapshot_column`, from the archive path) and the fetch
   its .meta.json sidecar recorded. For a comma-separated, double-quoted file, as get.data.gov.lt
   exports them. A column added in a later snapshot is null in earlier ones. #}
{% macro archived_csv(source, name, model_name, snapshot_column) %}
    {%- set snapshots = archived_snapshots(model_name, source) -%}
with published as (
    select *
    from read_csv(
        {{ archived_files(source, name, snapshots) }},
        delim = ',',
        quote = '"',
        escape = '"',
        header = true,
        all_varchar = true,  -- types are given once, in staging
        union_by_name = true,
        filename = '_source_file'
    )
),

fetches as (
    select _meta_file, url, fetched_at, last_modified, sha256
    from read_json(
        {{ archived_files(source, name ~ '.meta.json', snapshots) }},
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

{# Every entry of the archived JSON pages matching `pattern` of the snapshots a build of
   `model_name` reads (archived_snapshots), as get.data.gov.lt sends them ({"_data": [...]}): one
   row per entry, the entry as published in `entry` (JSON), plus the file it was read from, its
   snapshot (in `snapshot_column`, from the archive path) and the fetch its .meta.json sidecar
   recorded. A snapshot may hold no such file, as a day with no changes holds none; it then has
   no rows. #}
{% macro archived_json_entries(source, pattern, model_name, snapshot_column) %}
    {%- set snapshots = archived_snapshots(model_name, source) -%}
    {%- set files = existing_archived_files(source, pattern, snapshots) -%}
    {%- set sidecars = [] -%}
    {%- for file in files -%}
        {%- do sidecars.append(file ~ '.meta.json') -%}
    {%- endfor -%}
    {%- if files %}
with published as (
    select unnest(_data) as entry, _source_file
    from read_json(
        {{ files }},
        columns = {_data: 'json[]'},
        format = 'unstructured',
        maximum_object_size = 67108864,  -- a page of 10,000 changes is about 3 MB
        filename = '_source_file'
    )
),

fetches as (
    select _meta_file, url, fetched_at, last_modified, sha256
    from read_json(
        {{ sidecars }},
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
    published.entry,
    published._source_file,
    regexp_extract(published._source_file, '/([^/]+)/[^/]+$', 1) as {{ snapshot_column }},
    fetches.url as _file_url,
    fetches.fetched_at as _file_fetched_at,
    fetches.last_modified as _file_last_modified,
    fetches.sha256 as _file_sha256
from published
left join fetches
    on published._source_file || '.meta.json' = fetches._meta_file
    {%- else %}
select
    null::json as entry,
    null::varchar as _source_file,
    null::varchar as {{ snapshot_column }},
    null::varchar as _file_url,
    null::varchar as _file_fetched_at,
    null::varchar as _file_last_modified,
    null::varchar as _file_sha256
where false
    {%- endif %}
{% endmacro %}

{# The archived files matching `name` (a glob) in each of the snapshots given, that exist, not
   counting sidecars. Found when dbt executes; when it parses, the globs stand in for them. #}
{% macro existing_archived_files(source, name, snapshots) -%}
    {%- set globs = archived_files(source, name, snapshots) -%}
    {%- if not execute -%}
        {{- return(globs) -}}
    {%- endif -%}
    {%- set found -%}
        select file from glob({{ globs }})
        where not ends_with(file, '.meta.json')
        order by file
    {%- endset -%}
    {{- return(run_query(found).columns[0].values() | list) -}}
{%- endmacro %}

{# The snapshots a build of `model_name` reads, for a model that keeps one slice per snapshot of
   `source`: this build's, or every complete snapshot when the model's table is built from
   nothing, because it does not exist yet or the build is a --full-refresh. A rebuild then loses
   no snapshot the archive holds, as the archive is what raw is built from. #}
{% macro archived_snapshots(model_name, source) -%}
    {%- if not execute -%}
        {{- return([snapshot(source)]) -}}
    {%- endif -%}
    {%- set node = graph.nodes.values()
        | selectattr('resource_type', 'equalto', 'model')
        | selectattr('name', 'equalto', model_name)
        | first -%}
    {%- set table = adapter.get_relation(node.database, node.schema, node.alias) -%}
    {%- if table is not none and not flags.FULL_REFRESH -%}
        {{- return([snapshot(source)]) -}}
    {%- endif -%}
    {%- set snapshots = complete_snapshots(source) -%}
    {%- if not snapshots -%}
        {{ exceptions.raise_compiler_error("The archive holds no complete snapshot of " ~ source) }}
    {%- endif -%}
    {{- return(snapshots) -}}
{%- endmacro %}
