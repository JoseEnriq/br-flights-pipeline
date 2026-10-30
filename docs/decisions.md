# Decisions

Every decision in the project, with the reason behind it and the alternative it beat. Grouped by pipeline layer.

## Source

- **ANAC's VRA (Voo Regular Ativo).** Every scheduled flight in Brazil, one CSV per month since 2000, published about four weeks after the month ends. Chosen because it is a live source: a new month comes out every month and recent months are regenerated every day, which calls for a real incremental load. Alternatives compared: the Chamber of Deputies' expense quota (richer modeling, but it carries suppliers' CPF numbers and ships yearly files), ANP fuel prices (inconsistent file names), Receita Federal's CNPJ registry (heavy full snapshots, nothing incremental) and NYC Taxi (very common in portfolios).
- **No official data dictionary.** ANAC does not publish what the route type and authorization codes mean. The assumptions are in the column descriptions in `dbt/models/staging/_stg_models.yml`.
- **The data is not republished.** The bucket is private and no ANAC file goes into Git; the repository holds the code and aggregated numbers, always crediting the source.

## Ingestion (raw)

- **ELT: the raw layer keeps the file exactly as published**, with the BOM, the metadata line and nulls written as text. Cleaning happens in dbt, and a transformation bug is fixed by rebuilding from raw.
- **Changes are detected by content, not by date.** ANAC rewrites the `Atualizado em: <date>` line every day, even when no flight changes. The SHA-256 hash skips that line, so only a real change creates a new version.
- **One folder per version** (`raw/vra/month=YYYY-MM/ingestion_date=YYYY-MM-DD/`), with no overwrites and no S3 versioning. This makes it possible to compare two versions of the same month in Athena.
- **Partitioned by the month of the published file**, not by flight date: each file carries a few flights from the first days of the next month, but months never overlap.
- **State as JSON in the bucket itself** (`_state/vra_ingest.json`), written after each file. DynamoDB was ruled out: a few dozen months and a single writer.
- **A 13-month lookback window** (the current month and the 12 before it) on every run, and the full history with `--since`. Downloading every month every day would be waste, since old months almost never change.

## Raw catalog (Athena)

- **Table declared by hand, no Glue Crawler.** A crawler guesses the format by looking at the files and writes the table to the catalog on its own; it neither moves nor transforms data, it only replaces `CREATE TABLE` and `MSCK REPAIR`. On a non-standard file like ANAC's (BOM, metadata line before the header, `;` separator) it gets it wrong, and it would also change the table without review. A hand-written table is exact, free and versioned. Details in `athena/README.md`.
- **Experiment that confirmed the decision** (2026-10-30, test database deleted afterwards): two crawlers sharing one role, one over September 2026 in raw and one over the Parquet staging table.

  | | Crawler on the raw CSV | Crawler on the staging Parquet |
  |---|---|---|
  | Format detected | `UNKNOWN` | `parquet` |
  | Columns | none | all 13, with dates as `timestamp` |
  | Athena query | fails with `HIVE_UNSUPPORTED_FORMAT` | matches the dbt table (1,021,748 rows) |

  The CSV result was worse than expected: no columns at all instead of wrong ones. Glue's CSV classifier does try `;`, but it requires at least two columns on every line, and ANAC's first line (`Atualizado em: ...`) has only one; that, plus the BOM, is the most likely cause. A custom classifier can set the separator and the header, but it cannot skip the metadata line. Parquet carries its schema inside the file, so the crawler simply reads it. Crawlers are a good fit for formats like that, for well-behaved CSV and JSON, and for lakes with many tables or frequently changing schemas; not for non-standard files.
- **Every raw column is a string.** Date and null formats change between eras of the source; types belong to staging, where each rule is tested.
- **English column names, values as published.** Names without accents or spaces avoid quoting in every query; each column comment keeps the original ANAC name. Flight status, airports and airlines stay as ANAC publishes them.
- **Workgroup `br-flights` with a 1 GB per-query limit** and results in `_athena-results/`, deleted after 7 days by a lifecycle rule. Keeping results and data in the same bucket, with SSE-S3, is a simplification: companies usually keep results in a separate bucket and use SSE-KMS.
- **The workgroup does not enforce its settings on clients.** With *Override client-side settings* on, Athena ignores the dbt models' `external_location` and writes the tables inside `_athena-results/`, where they would be deleted after 7 days. This happened on the first staging run and was fixed by turning the option off.

## Staging (dbt + Athena)

- **dbt Core v1 with the `dbt-athena` adapter.** dbt v2 (2.0.9) rejects Athena: `unknown variant athena, expected one of redshift, snowflake, postgres, bigquery, trino, ...`. The `trino` adapter does not work either, because Athena is only reachable through the AWS API.
- **Python 3.12.** `dbt-athena` 1.11.1 requires `mmh3<4.2`, and `mmh3` 4.1.0 has no prebuilt Windows package for Python 3.13, so installing it would need a C++ compiler. `dbt-core` is pinned to the stable release (1.12.6): without the pin, pip picks a beta, because `dbt-athena` accepts release candidates.
- **Parquet with ZSTD compression.** Athena charges for compressed bytes read; ZSTD compresses better than Snappy, the most common default, and reads just as fast.
- **Incremental Hive table with `insert_overwrite`, partitioned by month.** The source changes a whole month at a time, and this strategy replaces the month's partition without mixing versions. Iceberg was ruled out for now: in `dbt-athena` it only supports `append` and `merge`, which need a unique key the source does not guarantee.
- **Only the latest version of each month, and only when it is not in staging yet.** The versions to process come from the catalog (`raw_vra$partitions`, which reads no data) and enter the SQL as literal filters, because Athena does not prune partitions through a subquery. When nothing changed, the filter becomes `false`: a second run in a row read 1.7 KB of metadata and wrote nothing.
- **`cast` instead of `try_cast` for dates.** An invalid date should break the model, not silently become null. February and March 2020 are corrupted at the source and will need explicit handling when the history is loaded.
- **Fully repeated rows are removed in staging** (`select distinct`): 18 out of 1,021,766 flights in the first 12 months.
- **The flight key is tested with a warning, not an error.** Airline, number and scheduled departure (or the actual one, for unscheduled flights) repeat in 122 cases with different actual times, such as ACA 0090 on 2025-10-27, recorded departing at 01:29 and at 23:08. Choosing the right row is a business rule and belongs to the flight model.
- **Flights whose origin equals their destination** (4,133, all completed) stay in staging; what they mean is decided in the flight model.

### CSV vs. Parquet

The same 12 months (October 2025 to September 2026), measured on 2026-10-30 with the queries in `athena/queries/csv_vs_parquet.sql`:

| | Raw (CSV) | Staging (Parquet) |
|---|---|---|
| Size in S3 | 144.8 MB | 13.2 MB |
| Flight status for September 2026 | 12.1 MB scanned | 3.7 KB scanned |
| Flights that departed, all months | 144.8 MB scanned | 5.0 MB scanned |

Parquet reads only the columns a query uses, compresses each column on its own (flight status has two values across 85 thousand rows) and stores dates as numbers instead of text.
