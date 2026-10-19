-- The risk_models registry's enabled rows and the project's rm_<id> models correspond one to one,
-- version by version, and each model keeps the framework's shape: its own group, public, with an
-- enforced contract. A row is a mismatch and what is wrong with it. fct_score_components reads
-- the registry, so a row naming no model fails dbt's parse before this runs; this catches the
-- other way, a model built but never scored, and a weight that is not positive.
{%- set models = [] %}
{%- if execute %}
    {%- for node in graph.nodes.values()
        if node.resource_type == 'model' and node.name.startswith('rm_') and '__' not in node.name %}
        {%- do models.append(node) %}
    {%- endfor %}
{%- endif %}

with models as (
    {%- for node in models %}
    select
        '{{ node.name[3:] }}' as id,
        {{ node.version if node.version is not none else 'null' }}::integer as version,
        {{ (node.group == node.name[3:]) | string | lower }} as in_own_group,
        {{ (node.access == 'public') | string | lower }} as is_public,
        {{ (node.contract.enforced) | string | lower }} as has_contract
    {% if not loop.last %}union all{% endif %}
    {%- else %}
    select null::varchar as id, null::integer as version, true as in_own_group,
        true as is_public, true as has_contract
    where false
    {%- endfor %}
),

registry as (
    select id, version, weight from {{ ref('risk_models') }} where enabled
)

select
    coalesce(models.id, registry.id) as id,
    coalesce(models.version, registry.version) as version,
    case
        when registry.id is null then 'a model with no enabled registry row'
        when models.id is null then 'an enabled registry row with no model'
        when not models.in_own_group then 'a model outside its group, named as its folder'
        when not models.is_public then 'a model that is not public'
        when not models.has_contract then 'a model without an enforced contract'
        when registry.weight <= 0 then 'a weight that is not positive'
    end as problem
from models
full join registry on models.id = registry.id and models.version = registry.version
where registry.id is null
    or models.id is null
    or not models.in_own_group
    or not models.is_public
    or not models.has_contract
    or registry.weight <= 0
