# Infrastructure

Terraform for the AWS resources of the pipeline: the lake bucket (`storage.tf`), the Glue databases and the Athena workgroup (`catalog.tf`), the least-privilege pipeline user and the alert topic (`pipeline.tf`). The state lives in the lake bucket itself (`versions.tf`).

## First run in a new account

The state cannot start in a bucket that does not exist yet, so the first apply runs on local state.

1. Set your bucket name as the default of `lake_bucket` in `variables.tf`; bucket names are global.
2. Switch the backend to local state with an override file, which Git ignores:

   ```bash
   printf 'terraform {\n  backend "local" {}\n}\n' > backend_override.tf
   terraform init
   terraform apply
   ```

3. To keep the state in the bucket, as the project does: delete `backend_override.tf`, set the same bucket name in the `backend "s3"` block of `versions.tf` and run `terraform init -migrate-state`.

## Outside Terraform

- The pipeline user's access key, created in the console, so the secret never lands in the state.
- The email subscription to the alert topic, which holds a personal address.
- The Athena tables, created by `pipeline/create_raw_tables.py` and dbt.

`terraform destroy` stops at the bucket on purpose (`prevent_destroy`): it holds every raw version ever downloaded. Empty and delete it in the console first.
