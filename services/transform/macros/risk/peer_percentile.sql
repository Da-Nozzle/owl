{# Ranks each row of `relation` among its peers, adding peer_group, peer_count and peer_percentile.
   `relation` (a CTE, usually) has one row per company and period_month, the column `value`, and
   the profile's activity_classification, activity_class, municipality_code and municipality.

   Peers are the companies the model judges in the same period and activity class (four digits of
   one EVRK version: 5611 restaurants, 9621 hairdressing), and with `by_municipality` the same
   municipality too, when that group has at least `min_peers` companies; else the class in the
   whole country; else none, and the percentile is empty. The class is the finest level that keeps
   most groups big enough (in 2026-08, 94% of Sodra's employers with a published wage are in a
   class of 20 or more, 85% of Vilnius's in a class of 20 or more there), and fine enough that a
   café is not compared with a hotel (division 55 against 56).

   peer_percentile is 0 to 100, higher more suspicious whichever way the value points: the share
   of the other peers that are strictly less suspicious (percent_rank). With `suspicious='lower'`
   (a lower value is worse, as a wage) 89 means 89% of the peers have a higher value. Ties share
   the lowest rank, so a value most peers share is never high. #}
{% macro peer_percentile(relation, value, suspicious='higher', by_municipality=true, min_peers=none) %}
    {%- set min_peers = min_peers if min_peers is not none else var('risk_min_peers') -%}
    {%- set order = {'higher': 'asc', 'lower': 'desc'}[suspicious] -%}
    {%- set class = 'period_month, activity_classification, activity_class' %}
    {%- set label = "activity_classification || ' ' || left(activity_class, 2) || '.' || right(activity_class, 2)" %}
    select
        * exclude (_class_peers, _class_rank, _local_peers, _local_rank),
        case
            when activity_class is null then null
            when _local_peers >= {{ min_peers }}
                then {{ label }} || ' in ' || municipality
            else {{ label }} || ' in Lithuania'
        end as peer_group,
        case
            when activity_class is null then null
            when _local_peers >= {{ min_peers }} then _local_peers
            else _class_peers
        end::integer as peer_count,
        case
            when activity_class is null then null
            when _local_peers >= {{ min_peers }} then round(100 * _local_rank, 1)
            when _class_peers >= {{ min_peers }} then round(100 * _class_rank, 1)
        end::double as peer_percentile
    from (
        select
            *,
            count(*) over (partition by {{ class }}) as _class_peers,
            percent_rank() over (
                partition by {{ class }} order by {{ value }} {{ order }}
            ) as _class_rank,
            {%- if by_municipality %}
            case when municipality_code is not null then
                count(*) over (partition by {{ class }}, municipality_code)
            end as _local_peers,
            percent_rank() over (
                partition by {{ class }}, municipality_code order by {{ value }} {{ order }}
            ) as _local_rank
            {%- else %}
            null::bigint as _local_peers,
            null::double as _local_rank
            {%- endif %}
        from {{ relation }}
    )
{% endmacro %}
