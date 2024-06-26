#create init.ini file for lambda function
resource "local_file" "init_file" {
  content = <<EOF
[step_functions]
step_functions_arn=${aws_sfn_state_machine.sfn_emr.arn}
  EOF
  filename = "./lambda_src/init.ini"
}

#create lambda function zip
data "archive_file" "lambda_zip" {
  type = "zip"
  source_dir = "./lambda_src"
  output_path = "./lambda_function.zip"
  depends_on = [ local_file.init_file ]
}

#create lambda function
resource "aws_lambda_function" "lambda_function"{
  filename = "./lambda_function.zip"
  function_name = "demo-trigger-sfn"
  role = aws_iam_role.lambda_execution_role.arn
  handler = "lambda_function.lambda_handler"
  runtime = "python3.11"
  timeout = 30
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  depends_on = [ data.archive_file.lambda_zip ]
}

#create lambda permission for bucket notification
resource "aws_lambda_permission" "allow_s3_notification" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_function.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.s3.arn
}