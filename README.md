# br-flights-pipeline

[![ci](https://github.com/JoseEnriq/br-flights-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/JoseEnriq/br-flights-pipeline/actions/workflows/ci.yml)

**English** | [Português](README.pt-BR.md)

A daily pipeline on AWS for every scheduled flight in Brazil, from ANAC's open data. It notices when ANAC publishes or quietly republishes a month, keeps every version it has seen, rebuilds only what changed and checks its own numbers against the delay percentages ANAC publishes.

## The problem

ANAC releases one flight file per month, about four weeks after the month ends, and regenerates recent files every day, sometimes changing flights, usually not. Old months change too: January 2019 was regenerated in May 2026. A project that loads a folder of files once never sees any of this. The files also have traps: a metadata line before the header, two months exported with every date replaced by the text `Microsoft.SqlServer.Dts.Pipeline.BlobColumn`, and delay percentages that count each codeshare flight number as a separate flight.

## How it works

```
ANAC ──► ingest (hash of the flights, one S3 folder per real version) ──► raw CSV
                                                                            │  Glue + Athena
         dbt: staging in Parquet, incremental by month ◄────────────────────┘
          └► flight fact, airline, airport and date dimensions, punctuality, reconciliation with ANAC

Airflow (Docker) runs it daily at 20:00 and emails through SNS when a task fails.
Terraform holds the AWS resources; GitHub Actions runs the tests and static checks.
```

A day without news ends after the downloads, with no Athena query. When a file changes, its partition is registered and dbt rebuilds the layers above it.

## Key decisions

All of them, with the alternatives I rejected, are in [docs/decisions.md](docs/decisions.md) (in Portuguese).

- **Change is detected by content, not by date.** The SHA-256 skips the `Atualizado em` line ANAC rewrites daily, so only a real change stores a new version.
- **dbt on Athena instead of Spark.** About 12 MB of CSV a month does not justify a cluster, and Athena charges by bytes read.
- **Broken months stay in raw and out of the model**, listed by hand in `dbt_project.yml`, so a new error of the same kind fails loudly instead of being filtered away.
- **One flight per row.** When a flight number operates twice around the same date, ANAC pairs every scheduled time with every actual one; each flight keeps the actual time closest to its schedule.
- **A least-privilege IAM user runs the pipeline**; no AWS credential is stored in GitHub.

## What the data shows

From the queries in [athena/queries/](athena/queries/) and the models in [dbt/](dbt/):

- **The pandemic:** 6,227 flights operated in April 2020, against 75,574 in April 2019 (−92%). Only April 2024 passed the 2019 level.
- **The model matches ANAC:** in the 24 months where ANAC's own percentages are a reliable reference, 95.0% to 99.7% of the airline, flight number and route combinations match its published counts and rates.
- **ANAC's percentages count codeshares:** August 2026 lists 208,412 scheduled legs for 85,713 scheduled flights. Adding up ANAC's percentages to count flights counts the same aircraft several times.
- **Parquet pays off:** 1.08 GB of raw CSV since 2019 became 92 MB of staged Parquet.
- **It costs cents:** US$ 0.31 of AWS usage from the start of the project to November 8, 2026, the history since 2019 included.

## What I left out

Streaming, since ANAC publishes files, not events; Spark and a data warehouse, which this volume does not need. Old raw versions cannot be rebuilt by anyone: ANAC only serves the current file, so they exist only in this project's bucket.

## Reproduce it

Requires Python 3.12, Terraform 1.10+, an AWS account and, for the daily run, Docker. Copy `.env.example` to `.env` and set `LAKE_BUCKET` to a bucket name of your own, which `infra/` also needs (`lake_bucket` and the backend block).

```bash
pip install -r requirements-dev.txt
cd infra && terraform init && terraform apply && cd ..   # a new account starts on local state
python pipeline/create_raw_tables.py
python pipeline/ingest.py --since 2019-01 && python pipeline/ingest_punctuality.py --since 2019-01 && python pipeline/ingest_stats.py
cd dbt && dotenv -f ../.env run -- dbt build && cd ..
cd airflow && docker compose up -d --build                # Airflow on http://localhost:8080
```

Airflow also needs an access key for the pipeline user, in an AWS profile named `br-flights-pipeline`; it is created in the console, outside Terraform. The first `dbt build` over the whole history reads more than the workgroup's 1 GB cutoff; [docs/decisions.md](docs/decisions.md) shows the batched load. Data: [ANAC open data](https://sistemas.anac.gov.br/dadosabertos/), not redistributed here.
