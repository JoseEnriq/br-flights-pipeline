-- The same questions asked of the raw CSV table and of the staged Parquet table.
-- Each pair returns the same answer; the "Data scanned" shown by Athena is what
-- docs/decisions.md compares. Measured on 2026-10-30 with 12 months loaded.

-- 1a. Flight status in September 2026, from the raw CSV (latest version, no repeated rows).
SELECT flight_status, count(*) AS flights
FROM (
    SELECT DISTINCT *
    FROM br_flights.raw_vra
    WHERE "month" = '2026-09'
      AND ingestion_date = '2026-10-28'
)
GROUP BY 1
ORDER BY 1;

-- 1b. The same question from the staged Parquet.
SELECT flight_status, count(*) AS flights
FROM br_flights_processed.stg_flights
WHERE "month" = '2026-09'
GROUP BY 1
ORDER BY 1;

-- 2a. Flights that actually departed, all months, from the raw CSV.
SELECT count(*) AS departed
FROM br_flights.raw_vra
WHERE nullif(actual_departure, 'null') IS NOT NULL;

-- 2b. The same question from the staged Parquet. Differs from 2a by the fully
--     repeated rows that staging removes.
SELECT count(*) AS departed
FROM br_flights_processed.stg_flights
WHERE actual_departure IS NOT NULL;
