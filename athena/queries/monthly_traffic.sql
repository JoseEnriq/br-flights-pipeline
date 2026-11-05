-- Flights, cancellations and delays by month since 2019, where the pandemic shows.
-- February and March 2020 are missing: ANAC published them with every date
-- replaced by text, and they stay out of staging (docs/decisions.md).
-- Line type X, used only from April 2020 to October 2021, counts in the total
-- but in none of the three groups; the meaning of each code is assumed, since
-- ANAC publishes no dictionary (dbt/models/staging/_stg_models.yml).
SELECT
    "month",
    count_if(actual_departure IS NOT NULL)                                AS operated_flights,
    count_if(actual_departure IS NOT NULL AND line_type = 'N')            AS domestic_flights,
    count_if(actual_departure IS NOT NULL AND line_type = 'I')            AS international_flights,
    count_if(actual_departure IS NOT NULL AND line_type IN ('C', 'G'))    AS cargo_flights,
    round(100.0 * count_if(is_scheduled AND is_cancelled) / count_if(is_scheduled), 1) AS cancelled_pct,
    round(100.0 * count_if(is_delayed_30) / count(is_delayed_30), 1)      AS delayed_30_pct
FROM br_flights_curated.fct_flights
GROUP BY "month"
ORDER BY "month";
