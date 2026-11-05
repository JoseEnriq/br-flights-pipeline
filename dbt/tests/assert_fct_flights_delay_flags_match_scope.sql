-- The punctuality flags exist exactly for scheduled flights with an actual
-- arrival. Not cancelled is not enough: NÃO INFORMADO flights and a few
-- operated ones have no actual arrival to measure.
select
    flight_id,
    is_scheduled,
    is_cancelled,
    is_delayed_30,
    is_delayed_60
from {{ ref('fct_flights') }}
where (is_scheduled and actual_arrival is not null) <> (is_delayed_30 is not null)
   or (is_delayed_30 is null) <> (is_delayed_60 is null)
