variable "region" {
  description = "Region for AWS resources."
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Profile to be utilized in local AWS CLI configuration"
  type        = string
  default     = "default"
}
