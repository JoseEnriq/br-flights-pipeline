# Terraform owns the databases only. The raw tables are declared by the reviewed
# DDL in athena/, and dbt rebuilds its own tables in the other two.
resource "aws_glue_catalog_database" "raw" {
  name        = "br_flights"
  description = "Brazilian scheduled flights (ANAC VRA) by layer"
}

resource "aws_glue_catalog_database" "processed" {
  name = "br_flights_processed"
}

resource "aws_glue_catalog_database" "curated" {
  name = "br_flights_curated"
}

resource "aws_athena_workgroup" "lake" {
  name        = "br-flights"
  description = "Queries over the br-flights data lake"

  configuration {
    # The cost guardrail: Athena cancels any query that would read more than 1 GB
    bytes_scanned_cutoff_per_query = 1073741824

    # When the workgroup enforces its settings, Athena ignores the location dbt
    # gives each table and writes it under _athena-results/, which expires
    enforce_workgroup_configuration    = false
    publish_cloudwatch_metrics_enabled = true

    engine_version {
      selected_engine_version = "AUTO"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/_athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
