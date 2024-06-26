#cloud watch log group for lambda function
resource "aws_cloudwatch_log_group" "lambda_log" {
  name = "/aws/lambda/demo-trigger-sfn"
  retention_in_days = 1
}