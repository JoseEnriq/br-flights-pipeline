# Athena

Catalog definitions and queries for the lake, run in the `br-flights` workgroup.

| File | What it does |
|---|---|
| `create_database.sql` | Creates the `br_flights` Glue database, shared by every layer |
| `raw_vra.sql` | Declares `raw_vra` over `s3://.../raw/vra/`, partitioned by `month` and `ingestion_date` |
| `repair_raw_vra.sql` | Registers the partition folders found in S3; run after each ingest that stores a new version |
| `queries/raw_checks.sql` | First checks on the raw data |

Run them in this order, one statement at a time.

## Why the table looks like this

- **Every column is a string.** The source changes date and null formats between eras, so typing belongs to staging, where each rule is tested.
- **Columns follow the source header order**, renamed to English; each column comment keeps the original ANAC name.
- **Two lines are skipped.** Line 1 is `Atualizado em: <date>` (with the BOM) and line 2 is the header.
- **Written by hand instead of a Glue crawler.** A crawler would read the metadata line as the header, guess types that change from month to month and alter the table without review.

## Quirks of Athena DDL

- **No comments in the DDL files.** The console sends DDL statements as a single line, so a `--` comment swallows the rest of the statement, and the DDL parser rejects `/* */`. The explanations live here instead.
- **The separator is written `'\;'`.** Athena splits the text into statements at every `;`, even inside quotes, so a bare `';'` cuts the statement in half. The SerDe reads `\;` back as `;`.
