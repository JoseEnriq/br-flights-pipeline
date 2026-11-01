{{
    config(
        materialized='table',
        table_type='hive',
        format='parquet',
        write_compression='zstd',
        partitioned_by=['month'],
        external_location='s3://' ~ env_var('LAKE_BUCKET') ~ '/curated/fct_flights/'
    )
}}

{#-
    Rebuilt in full on every run: staging is already Parquet, so the whole
    history costs a few MB of scan, and the duplicate rule below never has to
    look across what was built before.
-#}

with flights as (

    -- unscheduled flights are told apart by their actual departure, in a key
    -- space of their own, so they never collide with a scheduled flight that
    -- leaves in the same minute
    select
        *,
        scheduled_departure is not null                 as is_scheduled,
        coalesce(scheduled_departure, actual_departure) as key_departure
    from {{ ref('stg_flights') }}

),

ranked as (

    -- when one flight number operates twice around the same date, ANAC pairs
    -- every scheduled time with every actual time; each flight keeps the
    -- actual departure closest to its schedule
    select
        *,
        row_number() over (
            partition by airline_icao, flight_number, origin_icao, is_scheduled, key_departure
            order by
                abs(date_diff('second', scheduled_departure, actual_departure)) nulls last,
                actual_departure nulls last,
                destination_icao,
                authorization_code
        ) as row_rank
    from flights

)

select
    -- a 64-bit number instead of an MD5 hex string: random text barely
    -- compresses, and the string version left the table 11 MB larger
    from_big_endian_64(xxhash64(to_utf8(concat_ws('|',
        airline_icao, flight_number, origin_icao,
        if(is_scheduled, 'scheduled', 'unscheduled'),
        cast(key_departure as varchar)
    ))))                                                       as flight_id,
    airline_icao,
    flight_number,
    authorization_code,
    line_type,
    origin_icao,
    destination_icao,
    cast(key_departure as date)                                as flight_date,
    scheduled_departure,
    actual_departure,
    scheduled_arrival,
    actual_arrival,
    date_diff('minute', scheduled_departure, actual_departure) as departure_delay_minutes,
    date_diff('minute', scheduled_arrival, actual_arrival)     as arrival_delay_minutes,
    is_scheduled,
    flight_status = 'CANCELADO'                                as is_cancelled,
    origin_icao = destination_icao                             as is_return_to_origin,
    -- ANAC's punctuality rule: arriving more than 30 (or 60) minutes late,
    -- counted only for scheduled flights that operated; null elsewhere
    if(is_scheduled and flight_status = 'REALIZADO',
        date_diff('minute', scheduled_arrival, actual_arrival) > 30) as is_delayed_30,
    if(is_scheduled and flight_status = 'REALIZADO',
        date_diff('minute', scheduled_arrival, actual_arrival) > 60) as is_delayed_60,
    ingestion_date,
    "month"
from ranked
where row_rank = 1
