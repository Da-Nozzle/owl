{{ config(materialized='table') }}

-- Every enabled risk model's rows (risk_models, through risk_model_components), with its family
-- and weight: a new model joins by its registry row alone.
with components as (
    {{ risk_model_components() }}
),

registry as (
    select id, version, family, weight from {{ ref('risk_models') }} where enabled
)

select
    components.* exclude (is_scored),
    registry.family,
    registry.weight,
    components.is_scored
from components
left join registry
    on components.risk_model = registry.id
    and components.risk_model_version = registry.version
