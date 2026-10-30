-- A flight is its airline, number and scheduled departure; flights outside
-- the schedule have no scheduled departure and use the actual one instead.
-- Warns instead of failing: the source repeats some keys with different
-- actual times (122 keys in the first 12 months loaded), and choosing which
-- row is right belongs to the flight model, not to staging.
{{ config(severity='warn') }}

select
    airline_icao,
    flight_number,
    coalesce(scheduled_departure, actual_departure) as departure,
    count(*) as rows_with_key
from {{ ref('stg_flights') }}
group by 1, 2, 3
having count(*) > 1
