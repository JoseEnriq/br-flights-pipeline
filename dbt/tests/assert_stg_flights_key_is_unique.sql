-- A flight is its airline, number, origin and scheduled departure; flights
-- outside the schedule use the actual departure instead, kept apart so they
-- never match a scheduled flight leaving in the same minute.
-- Warns instead of failing: the source repeats some keys with different
-- actual times (102 keys in the first 12 months loaded), and fct_flights
-- chooses which row is right.
{{ config(severity='warn') }}

select
    airline_icao,
    flight_number,
    origin_icao,
    scheduled_departure is not null as is_scheduled,
    coalesce(scheduled_departure, actual_departure) as departure,
    count(*) as rows_with_key
from {{ ref('stg_flights') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
