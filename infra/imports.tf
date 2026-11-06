# Every resource in this folder was first created by hand in the console. These
# blocks adopted them into the state on the first apply, and they stay: once a
# resource is in the state they do nothing, and if the state is ever lost, a plan
# brings everything back in without recreating it.
locals {
  account_id = data.aws_caller_identity.current.account_id
}

import {
  to = aws_s3_bucket.lake
  id = var.lake_bucket
}

import {
  to = aws_s3_bucket_public_access_block.lake
  id = var.lake_bucket
}

import {
  to = aws_s3_bucket_ownership_controls.lake
  id = var.lake_bucket
}

import {
  to = aws_s3_bucket_server_side_encryption_configuration.lake
  id = var.lake_bucket
}

import {
  to = aws_s3_bucket_lifecycle_configuration.lake
  id = var.lake_bucket
}

import {
  to = aws_glue_catalog_database.raw
  id = "${local.account_id}:br_flights"
}

import {
  to = aws_glue_catalog_database.processed
  id = "${local.account_id}:br_flights_processed"
}

import {
  to = aws_glue_catalog_database.curated
  id = "${local.account_id}:br_flights_curated"
}

import {
  to = aws_athena_workgroup.lake
  id = "br-flights"
}

import {
  to = aws_iam_user.pipeline
  id = "br-flights-pipeline"
}

import {
  to = aws_iam_policy.pipeline
  id = "arn:aws:iam::${local.account_id}:policy/br-flights-pipeline"
}

import {
  to = aws_iam_user_policy_attachment.pipeline
  id = "br-flights-pipeline/arn:aws:iam::${local.account_id}:policy/br-flights-pipeline"
}

import {
  to = aws_sns_topic.alerts
  id = "arn:aws:sns:${var.region}:${local.account_id}:br-flights-alerts"
}
