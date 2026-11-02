{{
    config(
        materialized='table',
        table_type='hive',
        format='parquet',
        write_compression='zstd',
        partitioned_by=['month'],
        external_location='s3://' ~ env_var('LAKE_BUCKET') ~ '/curated/agg_route_punctuality/'
    )
}}

-- One row per month, airline and route, for scheduled flights only, which
-- also leaves out the returns to origin. Counts are kept next to the rates so
-- that months or routes can be added up again: a rate over a year is total
-- delayed over total operated, never the average of monthly rates.
with flights as (

    select
        "month",
        airline_icao,
        origin_icao,
        destination_icao,
        count(*)                                            as scheduled_flights,
        count_if(is_cancelled)                              as cancelled_flights,
        count(is_delayed_30)                                as operated_flights,
        count_if(is_delayed_30)                             as delayed_30_flights,
        count_if(is_delayed_60)                             as delayed_60_flights,
        approx_percentile(arrival_delay_minutes, 0.5)       as median_arrival_delay_minutes
    from {{ ref('fct_flights') }}
    where is_scheduled
    group by 1, 2, 3, 4

)

select
    flights.airline_icao,
    airline.airline_name,
    flights.origin_icao,
    origin.city                                             as origin_city,
    origin.state                                            as origin_state,
    flights.destination_icao,
    destination.city                                        as destination_city,
    destination.state                                       as destination_state,
    flights.scheduled_flights,
    flights.cancelled_flights,
    flights.operated_flights,
    flights.delayed_30_flights,
    flights.delayed_60_flights,
    flights.median_arrival_delay_minutes,
    cast(flights.cancelled_flights as double) / flights.scheduled_flights  as cancellation_rate,
    cast(flights.delayed_30_flights as double) / nullif(flights.operated_flights, 0) as delay_30_rate,
    cast(flights.delayed_60_flights as double) / nullif(flights.operated_flights, 0) as delay_60_rate,
    flights."month"
from flights
join {{ ref('dim_airline') }} as airline
    on flights.airline_icao = airline.airline_icao
join {{ ref('dim_airport') }} as origin
    on flights.origin_icao = origin.airport_icao
join {{ ref('dim_airport') }} as destination
    on flights.destination_icao = destination.airport_icao
