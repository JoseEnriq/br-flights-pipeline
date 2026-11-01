{{
    config(
        materialized='table',
        table_type='hive',
        format='parquet',
        write_compression='zstd',
        external_location='s3://' ~ env_var('LAKE_BUCKET') ~ '/curated/dim_date/'
    )
}}

with days as (

    -- Athena's sequence() only steps over timestamps, and a Hive table
    -- rejects their precision, so each step goes back to a date
    select cast(step as date) as day
    from unnest(
        sequence(timestamp '{{ var("calendar_start") }}', timestamp '{{ var("calendar_end") }}', interval '1' day)
    ) as t (step)

)

select
    day                       as date_day,
    year(day)                 as year,
    quarter(day)              as quarter,
    month(day)                as month_of_year,
    date_format(day, '%Y-%m') as year_month,
    day_of_month(day)         as day_of_month,
    -- ISO numbering: Monday is 1, Sunday is 7
    day_of_week(day)          as day_of_week,
    date_format(day, '%W')    as day_name,
    day_of_week(day) >= 6     as is_weekend
from days
