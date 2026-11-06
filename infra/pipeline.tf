data "aws_caller_identity" "current" {}

# The user Airflow runs as. Its access key stays out of Terraform, which would
# keep the secret in the state file: it is created in the console and lives only
# on the machine that runs Airflow.
resource "aws_iam_user" "pipeline" {
  name = "br-flights-pipeline"
}

resource "aws_iam_policy" "pipeline" {
  name = "br-flights-pipeline"
  policy = templatefile("${path.module}/pipeline-policy.json.tftpl", {
    bucket     = aws_s3_bucket.lake.bucket
    region     = var.region
    account_id = data.aws_caller_identity.current.account_id
  })
}

resource "aws_iam_user_policy_attachment" "pipeline" {
  user       = aws_iam_user.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}

# The e-mail subscription stays out too, so that no address reaches the
# repository: it is added once in the console and confirmed from the inbox.
resource "aws_sns_topic" "alerts" {
  name = "br-flights-alerts"
}
