terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "local" {}
}

module "aws" {
  source = "./modules/aws"

  region      = var.region
  aws_profile = var.aws_profile
  bucket_name = var.bucket_name
}
