-- Returns the flights whose times contradict their status or each other.
-- ANAC publishes 4 such flights since 2019: two AZA flights of October 2021
-- operated with no actual arrival, and two flights of February 2019 scheduled
-- to arrive before they leave. A few are source errors to list; many would
-- mean a format change.
{{ config(warn_if='>0', error_if='>10') }}

select
    airline_icao,
    flight_number,
    scheduled_departure,
    actual_departure,
    actual_arrival,
    flight_status,
    "month"
from {{ ref('stg_flights') }}
where
    (flight_status = 'CANCELADO' and (actual_departure is not null or actual_arrival is not null))
    or (flight_status = 'REALIZADO' and (actual_departure is null or actual_arrival is null))
    or (scheduled_departure is null and actual_departure is null)
    or actual_arrival < actual_departure
    or scheduled_arrival < scheduled_departure
