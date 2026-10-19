{# The enabled rows of the risk_models seed, as [{'id': 'wage_floor', 'version': 1}, ...]. dbt
   cannot read a seed while it parses, and a model's refs must be known then, so the launchers
   read the CSV and pass its enabled rows as OWL_RISK_MODELS ("wage_floor:1,..."; dbt/environment.py
   in the orchestration). Editing the CSV changes the variable, which makes dbt parse again. #}
{% macro risk_model_registry() %}
    {%- set registered = [] -%}
    {%- for entry in env_var('OWL_RISK_MODELS').split(',') if entry.strip() -%}
        {%- set id, version = entry.strip().split(':') -%}
        {%- do registered.append({'id': id, 'version': version | int}) -%}
    {%- endfor -%}
    {{- return(registered) -}}
{% endmacro %}

{# Whether a registered version is its model's latest_version, the one fct_company_risk scores: a
   v2 above it runs beside v1 in fct_score_components until its YAML makes it the latest. Read
   from the graph, so false while dbt parses; a unit test has no graph, and scores every version. #}
{% macro risk_model_is_scored(id, version) %}
    {%- if execute -%}
        {%- if 'nodes' not in graph -%}
            {{- return(true) -}}
        {%- endif -%}
        {%- for node in graph.nodes.values() -%}
            {%- if node.resource_type == 'model' and node.name == 'rm_' ~ id
                and node.version | string == version | string -%}
                {{- return((node.latest_version or node.version) | string == version | string) -}}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {{- return(false) -}}
{% endmacro %}

{# The versions scored, "wage_floor@1,...": stamped on every row of fct_company_risk. #}
{% macro risk_scored_versions() %}
    {%- set scored = [] -%}
    {%- for model in risk_model_registry() | sort(attribute='id') -%}
        {%- if risk_model_is_scored(model.id, model.version) -%}
            {%- do scored.append(model.id ~ '@' ~ model.version) -%}
        {%- endif -%}
    {%- endfor -%}
    {{- return(scored | join(',')) -}}
{% endmacro %}

{# The columns every rm_<id> holds, in order: its contract (README, Adding a risk model). #}
{% macro risk_model_columns() %}
    {{- return([
        'company_code', 'period_month', 'risk_model', 'risk_model_version', 'raw_value',
        'peer_group', 'peer_count', 'peer_percentile', 'fired', 'evidence', 'explanation',
        'source_as_of',
    ]) -}}
{% endmacro %}

{# Every enabled registered version's rows, one select each, with whether it is scored. #}
{% macro risk_model_components() %}
    {%- set registered = risk_model_registry() -%}
    {%- if not registered %}
    -- No risk model is enabled: no rows, typed as the contract.
    select
        null::bigint as company_code, null::date as period_month, null::varchar as risk_model,
        null::integer as risk_model_version, null::double as raw_value,
        null::varchar as peer_group, null::integer as peer_count, null::double as peer_percentile,
        null::boolean as fired, null::json as evidence, null::varchar as explanation,
        null::map(varchar, date) as source_as_of, false as is_scored
    where false
    {%- endif %}
    {%- for model in registered %}
    select
        {{ risk_model_columns() | join(', ') }},
        {{ risk_model_is_scored(model.id, model.version) }} as is_scored
    from {{ ref('rm_' ~ model.id, v=model.version) }}
    {% if not loop.last %}union all{% endif %}
    {%- endfor %}
{% endmacro %}
