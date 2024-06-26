#Create S3 bucket
resource "aws_s3_bucket" "s3" {
  bucket = "ntanvi-sfn-emr-demo"
}

#Upload scripts
resource "aws_s3_object" "s3-iceberg" {
  bucket = aws_s3_bucket.s3.id
  key    = "scripts/pyspark_iceberg.py"
  source = "./scripts/pyspark_iceberg.py"
  etag = filemd5("./scripts/pyspark_iceberg.py")
}

resource "aws_s3_object" "s3-hive" {
  bucket = aws_s3_bucket.s3.id
  key    = "scripts/pyspark_hive.py"
  source = "./scripts/pyspark_hive.py"
  etag = filemd5("./scripts/pyspark_hive.py")
}

#Upload sample data
resource "aws_s3_object" "s3-sample" {
  bucket = aws_s3_bucket.s3.id
  key    = "data_source/product_data.csv"
  source = "./data_samples/product_data.csv"
  etag = filemd5("./data_samples/product_data.csv")
  
}

#create s3 notification
resource "aws_s3_bucket_notification" "s3-trigger-lambda" {
  bucket = aws_s3_bucket.s3.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_function.arn
    events = [ "s3:ObjectCreated:*" ]
    filter_prefix = "data_source/"
    filter_suffix = ".csv"
  }
  depends_on = [ aws_lambda_permission.allow_s3_notification ]
}