# Athena

Catalog definitions and queries for the lake, run in the `br-flights` workgroup.

| File | What it does |
|---|---|
| `raw_vra.sql` | Declares `raw_vra` over `s3://.../raw/vra/`, partitioned by `month` and `ingestion_date` |
| `raw_anac_stats.sql` | Declares `raw_anac_stats` over `s3://.../raw/anac_stats/`, partitioned by `ingestion_date` |
| `raw_anac_punctuality.sql` | Declares `raw_anac_punctuality` over `s3://.../raw/anac_punctuality/`, ANAC's monthly delay percentages (Annex I) |
| `queries/raw_checks.sql` | First checks on the raw data |
| `queries/csv_vs_parquet.sql` | The same questions asked of the raw CSV and of the staged Parquet, to compare the data scanned |
| `queries/monthly_traffic.sql` | Flights by group, cancellations and delays by month since 2019, where the pandemic shows |

The Glue databases come from Terraform (`infra/catalog.tf`). The three `raw_*.sql` files write the bucket as `${lake_bucket}`: `pipeline/create_raw_tables.py` fills it from `LAKE_BUCKET`, creates the tables and registers the partitions already in S3, and `pipeline/repair_partitions.py` registers new ones after each ingest (the Airflow DAG runs it). To run a file in the console instead, replace `${lake_bucket}` with the bucket name first.

## Why the table looks like this

- **Every column is a string.** The source changes date and null formats between eras, so typing belongs to staging, where each rule is tested.
- **Columns follow the source header order**, renamed to English; each column comment keeps the original ANAC name.
- **Two lines are skipped.** Line 1 is `Atualizado em: <date>` (with the BOM) and line 2 is the header.
- **Written by hand instead of a Glue crawler.** A crawler would read the metadata line as the header, guess types that change from month to month and alter the table without review.

## Quirks of Athena DDL

- **No comments in the DDL files.** The console sends DDL statements as a single line, so a `--` comment swallows the rest of the statement, and the DDL parser rejects `/* */`. The explanations live here instead.
- **The separator is written `'\;'`.** Athena splits the text into statements at every `;`, even inside quotes, so a bare `';'` cuts the statement in half. The SerDe reads `\;` back as `;`.
