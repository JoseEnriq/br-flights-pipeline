-- The punctuality flags exist exactly for scheduled flights that operated,
-- which always carry both arrival times.
select
    flight_id,
    is_scheduled,
    is_cancelled,
    is_delayed_30,
    is_delayed_60
from {{ ref('fct_flights') }}
where (is_scheduled and not is_cancelled) <> (is_delayed_30 is not null)
   or (is_delayed_30 is null) <> (is_delayed_60 is null)
