{{
    config(
        materialized='table',
        table_type='hive',
        format='parquet',
        write_compression='zstd',
        external_location='s3://' ~ env_var('LAKE_BUCKET') ~ '/curated/audit_anac_reconciliation/'
    )
}}

-- Compares, month by month, the flight model with the percentages ANAC
-- publishes for each airline, flight number and route. A flight matches when
-- the scheduled count is the same and every percentage, rounded like ANAC's,
-- is within 0.01 point. ANAC also lists codeshare flight numbers, which the
-- flight files do not carry, so those show up as "only ANAC" and stay out of
-- the match rate.
with ours as (

    select
        "month",
        airline_icao,
        flight_number,
        origin_icao,
        destination_icao,
        count(*)                as scheduled_flights,
        count_if(is_cancelled)  as cancelled_flights,
        count(is_delayed_30)    as operated_flights,
        count_if(is_delayed_30) as delayed_30_flights,
        count_if(is_delayed_60) as delayed_60_flights
    from {{ ref('fct_flights') }}
    where is_scheduled
    group by 1, 2, 3, 4, 5

),

anac as (

    select *
    from {{ ref('stg_punctuality') }}
    -- only months the flight model also has
    where "month" in (select distinct "month" from {{ ref('fct_flights') }})

),

compared as (

    select
        coalesce(ours."month", anac."month") as "month",
        ours.scheduled_flights               as our_scheduled_flights,
        anac.scheduled_flights               as anac_scheduled_flights,
        ours.scheduled_flights is not null
            and anac.scheduled_flights is not null
                                             as is_in_both,
        coalesce(
            ours.scheduled_flights = anac.scheduled_flights
            and abs(round(100.0 * ours.cancelled_flights / ours.scheduled_flights, 2) - coalesce(anac.pct_cancelled, 0)) <= 0.01
            and abs(coalesce(round(100.0 * ours.delayed_30_flights / nullif(ours.operated_flights, 0), 2), 0) - coalesce(anac.pct_delayed_30, 0)) <= 0.01
            and abs(coalesce(round(100.0 * ours.delayed_60_flights / nullif(ours.operated_flights, 0), 2), 0) - coalesce(anac.pct_delayed_60, 0)) <= 0.01,
            false
        )                                    as is_match
    from ours
    full outer join anac
        on ours."month" = anac."month"
        and ours.airline_icao = anac.airline_icao
        and ours.flight_number = anac.flight_number
        and ours.origin_icao = anac.origin_icao
        and ours.destination_icao = anac.destination_icao

)

select
    "month",
    count_if(is_in_both)                                               as flight_routes_in_both,
    count_if(is_match)                                                 as flight_routes_matching,
    cast(count_if(is_match) as double) / count_if(is_in_both)          as match_rate,
    count_if(our_scheduled_flights is null)                            as flight_routes_only_anac,
    count_if(anac_scheduled_flights is null)                           as flight_routes_only_ours,
    sum(anac_scheduled_flights)                                        as anac_scheduled_legs,
    sum(our_scheduled_flights)                                         as our_scheduled_legs
from compared
-- months ANAC has not published yet have nothing to compare
group by 1
having count_if(anac_scheduled_flights is not null) > 0
