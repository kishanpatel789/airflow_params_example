provider "aws" {
  region  = var.region
  profile = var.aws_profile
}

data "aws_caller_identity" "current" {}

locals {
  aws_account_id = data.aws_caller_identity.current.account_id
}

# s3 bucket for python files
resource "aws_s3_bucket" "bucket_file_repo" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "encryption_data_lake" {
  bucket = aws_s3_bucket.bucket_file_repo.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lifecycle_data_lake" {
  bucket = aws_s3_bucket.bucket_file_repo.id

  rule {
    id = "lifecycle-global"

    filter {}

    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 60
      storage_class = "GLACIER"
    }

    expiration {
      days = 120
    }
  }
}