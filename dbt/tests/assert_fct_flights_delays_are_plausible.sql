-- Arrivals more than a day off schedule are kept as published, since ANAC
-- counts them in its own percentages, but they deserve a look: early ones are
-- data errors, late ones may be real (262 flights in the first 12 months).
{{ config(severity='warn') }}

select
    flight_id,
    airline_icao,
    flight_number,
    scheduled_arrival,
    actual_arrival,
    arrival_delay_minutes
from {{ ref('fct_flights') }}
where abs(arrival_delay_minutes) > 24 * 60
