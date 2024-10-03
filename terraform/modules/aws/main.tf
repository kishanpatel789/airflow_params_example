provider "aws" {
  region  = var.region
  profile = var.aws_profile
}

data "aws_caller_identity" "current" {}

locals {
  aws_account_id   = data.aws_caller_identity.current.account_id
  python_file_path = "${path.root}/src/python_file_to_run.py"
}

# s3 bucket for python files
resource "aws_s3_bucket" "bucket_file_repo" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "encryption_file_repo" {
  bucket = aws_s3_bucket.bucket_file_repo.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lifecycle_file_repo" {
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

resource "aws_s3_object" "python_file" {
  bucket = aws_s3_bucket.bucket_file_repo.bucket
  key    = "python_file_to_run.py"
  source = local.python_file_path

  etag = filemd5(local.python_file_path)
}
