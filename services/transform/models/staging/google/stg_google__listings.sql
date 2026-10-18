{{ config(materialized='table') }}  -- so a value that does not parse fails the build

{% set week_minutes = 7 * 24 * 60 %}

with found as (
    select * from {{ ref('raw_google_places__places') }}
),

-- A place on several searches' pages comes once per page; the copies differ only by the minutes
-- between their requests, so the last fetched is kept.
places as (
    select
        id,
        _snapshot_month,
        arg_max(place, _file_fetched_at) as place,
        max(_file_fetched_at) as fetched_at
    from found
    group by id, _snapshot_month
),

parsed as (
    select
        *,
        from_json(
            place -> 'addressComponents',
            '[{"longText": "VARCHAR", "types": ["VARCHAR"]}]'
        ) as address_components,
        from_json(
            place -> 'regularOpeningHours' -> 'periods',
            '[{"open": {"day": "INTEGER", "hour": "INTEGER", "minute": "INTEGER"},
               "close": {"day": "INTEGER", "hour": "INTEGER", "minute": "INTEGER"}}]'
        ) as opening_periods
    from places
)

select
    id as place_id,
    place -> 'displayName' ->> 'text' as name,
    place ->> 'primaryType' as primary_type,
    from_json(place -> 'types', '["VARCHAR"]') as types,
    place ->> 'businessStatus' as business_status,
    place ->> 'formattedAddress' as formatted_address,
    list_filter(address_components, part -> list_contains(part.types, 'route'))[1].longText
        as street,
    list_filter(address_components, part -> list_contains(part.types, 'street_number'))[1].longText
        as house_number,
    list_filter(address_components, part -> list_contains(part.types, 'postal_code'))[1].longText
        as postcode,
    list_filter(address_components, part -> list_contains(part.types, 'locality'))[1].longText
        as locality,
    (place -> 'location' ->> 'latitude')::double as latitude,
    (place -> 'location' ->> 'longitude')::double as longitude,
    (place ->> 'rating')::decimal(2, 1) as rating,
    (place ->> 'userRatingCount')::integer as review_count,
    place -> 'regularOpeningHours' as regular_opening_hours,
    -- Open around the clock is one period with no close. A period may close on a later day, or
    -- wrap from Saturday to Sunday (day 0), hence the modulo.
    case
        when list_bool_or(list_transform(opening_periods, period -> period.close is null))
            then 168
        else list_sum(list_transform(
            opening_periods,
            period -> (
                (period.close.day - period.open.day) * 1440
                + (period.close.hour - period.open.hour) * 60
                + coalesce(period.close.minute, 0) - coalesce(period.open.minute, 0)
                + {{ week_minutes }}
            ) % {{ week_minutes }}
        )) / 60
    end::decimal(5, 2) as weekly_open_hours,
    place ->> 'websiteUri' as website,
    place ->> 'nationalPhoneNumber' as phone,
    place ->> 'googleMapsUri' as google_maps_uri,
    nullif(regexp_extract(place ->> 'googleMapsUri', '[?&]cid=([0-9]+)', 1), '')::ubigint as cid,
    _snapshot_month as snapshot_month,
    fetched_at::timestamptz at time zone 'UTC' as fetched_at
from parsed
