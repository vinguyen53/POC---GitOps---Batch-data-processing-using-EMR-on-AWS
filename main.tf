#use AWS Cloud
provider "aws" {
  region = "ap-southeast-1"
  profile = "ntanvi"
}

#Setup S3 remote backend
terraform {
  backend "s3" {
    bucket = "ntanvi-emr"
    key = "tf-folder/emr-demo.tfstates"
    region = "ap-southeast-1"
    profile = "ntanvi"
  }
}

#account info
data "aws_caller_identity" "current" {}
