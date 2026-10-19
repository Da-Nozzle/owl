-- A profession code matches one prefix of profession_categories at most, or a vacancy would be
-- counted in two occupations.
select shorter.profession_code_prefix, longer.profession_code_prefix as overlapping_prefix
from {{ ref('profession_categories') }} as shorter
inner join {{ ref('profession_categories') }} as longer
    on starts_with(longer.profession_code_prefix, shorter.profession_code_prefix)
    and longer.profession_code_prefix <> shorter.profession_code_prefix
