-- First checks on the raw table, one query at a time in the Athena editor. The data
-- scanned by each one is the CSV baseline for the Parquet comparison in stage 3.

-- 1. Flights per stored version. Should match the "rows" kept in _state/vra_ingest.json.
SELECT "month", ingestion_date, count(*) AS flights
FROM br_flights.raw_vra
GROUP BY 1, 2
ORDER BY 1, 2;

-- 2. The two skipped lines really were skipped: no metadata or header text in the data.
SELECT count(*) AS leaked_lines
FROM br_flights.raw_vra
WHERE airline_icao LIKE '%Atualizado em%'
   OR airline_icao = 'ICAO Empresa Aérea';

-- 3. How the last column ends. Lines end in CRLF, and a trailing carriage return here
--    would have to be trimmed in staging.
SELECT justification_code,
       length(justification_code) AS chars,
       count(*) AS flights
FROM br_flights.raw_vra
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10;

-- 4. Flight status in the latest version of one month, reading a single partition.
--    The latest version comes from "raw_vra$partitions", the catalog's list of partitions,
--    which costs nothing; asking raw_vra itself would read the month's files once more.
SELECT flight_status, count(*) AS flights
FROM br_flights.raw_vra
WHERE "month" = '2026-09'
  AND ingestion_date = (
      SELECT max(ingestion_date) FROM br_flights."raw_vra$partitions" WHERE "month" = '2026-09'
  )
GROUP BY 1
ORDER BY 2 DESC;

-- 5. Only the latest version of each month: the rule staging will use.
SELECT "month", count(*) AS flights
FROM (
    SELECT *, max(ingestion_date) OVER (PARTITION BY "month") AS latest_ingestion
    FROM br_flights.raw_vra
)
WHERE ingestion_date = latest_ingestion
GROUP BY 1
ORDER BY 1;

-- 6. What changed between two versions of the same month: flights present in one
--    version and not in the other. Empty until ANAC republishes a month with changes.
WITH versions AS (
    SELECT ingestion_date,
           row_number() OVER (ORDER BY ingestion_date DESC) AS recency
    FROM br_flights."raw_vra$partitions"
    WHERE "month" = '2026-09'
),
newest AS (
    SELECT * FROM br_flights.raw_vra
    WHERE "month" = '2026-09'
      AND ingestion_date = (SELECT ingestion_date FROM versions WHERE recency = 1)
),
previous AS (
    SELECT * FROM br_flights.raw_vra
    WHERE "month" = '2026-09'
      AND ingestion_date = (SELECT ingestion_date FROM versions WHERE recency = 2)
)
SELECT 'only in newest' AS side, * FROM (
    SELECT airline_icao, flight_number, scheduled_departure, actual_departure, flight_status FROM newest
    EXCEPT
    SELECT airline_icao, flight_number, scheduled_departure, actual_departure, flight_status FROM previous
)
-- With a single version, "previous" is empty and every flight would look new.
WHERE EXISTS (SELECT 1 FROM versions WHERE recency = 2)
UNION ALL
SELECT 'only in previous' AS side, * FROM (
    SELECT airline_icao, flight_number, scheduled_departure, actual_departure, flight_status FROM previous
    EXCEPT
    SELECT airline_icao, flight_number, scheduled_departure, actual_departure, flight_status FROM newest
);
