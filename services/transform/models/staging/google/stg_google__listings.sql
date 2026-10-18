-- Google's listings mapped to names and types: one row per place and snapshot, as the Places API
-- sent it in that snapshot's sweep. A value that does not parse fails here; the fields not used
-- yet are in raw_google_places__places. A table, so every value is cast when it is built.
{{ config(materialized='table') }}

-- Minutes in a week, as opening hours are given per weekday (0 is Sunday).
{% set week_minutes = 7 * 24 * 60 %}

with found as (
    select * from {{ ref('raw_google_places__places') }}
),

-- A place on the pages of several searches paid for (a tile and its quarter, or two queries) was
-- sent once per page; the copies of one sweep differ by at most the minutes between their
-- requests, so the last fetched is kept. Which queries found a place is not known here: the sweep
-- pays for a place once, whatever found it, and keeps only the pages it paid for.
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
    -- A place open around the clock has one period with no close. Otherwise each period runs from
    -- its open to its close, which may be on a later day, and on Sunday after Saturday.
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
    -- The listing's customer id, which Maps pages carry as the second half of a feature id
    -- (0x...:0x<cid>), so a scraped listing can be matched to this one.
    nullif(regexp_extract(place ->> 'googleMapsUri', '[?&]cid=([0-9]+)', 1), '')::ubigint as cid,
    _snapshot_month as snapshot_month,
    fetched_at::timestamptz at time zone 'UTC' as fetched_at
from parsed
