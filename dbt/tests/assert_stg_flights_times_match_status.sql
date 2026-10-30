-- Returns the flights whose times contradict their status or each other.
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
