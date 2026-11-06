# Versioning stays off: the ingest rewrites a month whenever ANAC republishes it,
# and versions would keep a paid copy of every file replaced. The state does not
# need it either, since the import blocks can rebuild it.
resource "aws_s3_bucket" "lake" {
  bucket = var.lake_bucket

  # It holds every ANAC file since 2019 and this state; deleting it takes a
  # deliberate edit here first
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled       = true
    blocked_encryption_types = ["SSE-C"]
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  # Athena writes a copy of every result here; nothing reads them after a week
  rule {
    id     = "expire-athena-results"
    status = "Enabled"

    filter {
      prefix = "_athena-results/"
    }

    expiration {
      days = 7
    }
  }
}
