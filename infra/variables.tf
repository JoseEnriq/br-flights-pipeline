variable "region" {
  type    = string
  default = "us-east-1"
}

variable "lake_bucket" {
  description = "Bucket names are global, so another account needs a name of its own"
  type        = string
  default     = "br-flights-lake-joseenriq"
}
