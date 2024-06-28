# POC-GitOps-Batch-data-processing-using-EMR-on-AWS
## Data Architecture
<img width="697" alt="image" src="https://github.com/vinguyen53/POC-GitOps-Batch-data-processing-using-EMR-on-AWS/assets/140522361/a1e6287d-2af9-477d-86f6-77325ce03e74">

- Data source: S3 bucket, data format csv
- When a data file is put into S3 bucket, S3 send a notification event to trigger Lambda function
- Lambda function use boto3 library to trigger Step Functions with argument is input file url which get from S3 notification event
- Step Funtions execute following sequence: create EMR cluster, submit Spark job, terminate EMR cluster after job done
- EMR cluster is created with Spark, using Glue catalog for data catalog
- After read and transform by Pyspark job, EMR write data into S3 bucket with Iceberg table format
- Using Athena for query data

## GitOps Flow
<img width="682" alt="image" src="https://github.com/vinguyen53/POC-GitOps-Batch-data-processing-using-EMR-on-AWS/assets/140522361/b2058d04-bebc-4618-bf40-fe45d503e23d">

- Git: using Github
- CI/CD: using Github Actions
- Infrastructure as Code: using Terraform, including:
  + VPC: build vpc, subnet, route table, security group, internet gateway, vpc endpoint for EMR
  + IAM: build execution role for Lambda Function, EMR and instance profile for EC2
  + S3: build S3 bucket for data and scripts storage
  + Step Functions: build Step Functions to orchestrate EMR
  + Lambda Function: build Lambda Funtion
  + CloudWatch: build CloudWatch log group for Lambda Function
