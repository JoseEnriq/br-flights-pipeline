terraform {
  # 1.10 brought state locking in S3 itself (use_lockfile), without a DynamoDB table
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # The state lives in the lake bucket, under a prefix the pipeline user cannot
  # reach, so every machine shares one state and the lock stops two applies at
  # once. A backend block cannot read variables: another account edits it here.
  backend "s3" {
    bucket       = "br-flights-lake-joseenriq"
    key          = "_terraform/infra.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project = "br-flights-pipeline"
    }
  }
}
