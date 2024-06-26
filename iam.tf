#EMR service role
data "aws_iam_policy_document" "emr_service_role_assume" {
  statement {
    actions = [ "sts:AssumeRole" ]
    effect = "Allow"
    principals {
      type = "Service"
      identifiers = [ "elasticmapreduce.amazonaws.com" ]
    }
  }
}

data "aws_iam_policy_document" "emr_service_role_policy" {
    statement {
      effect = "Allow"
      actions = [ 
        "ec2:AuthorizeSecurityGroupEgress",
        "ec2:AuthorizeSecurityGroupIngress",
        "ec2:CancelSpotInstanceRequests",
        "ec2:CreateFleet",
        "ec2:CreateLaunchTemplate",
        "ec2:CreateNetworkInterface",
        "ec2:CreateSecurityGroup",
        "ec2:CreateTags",
        "ec2:DeleteLaunchTemplate",
        "ec2:DeleteNetworkInterface",
        "ec2:DeleteSecurityGroup",
        "ec2:DeleteTags",
        "ec2:DescribeAvailabilityZones",
        "ec2:DescribeAccountAttributes",
        "ec2:DescribeDhcpOptions",
        "ec2:DescribeImages",
        "ec2:DescribeInstanceStatus",
        "ec2:DescribeInstances",
        "ec2:DescribeKeyPairs",
        "ec2:DescribeLaunchTemplates",
        "ec2:DescribeNetworkAcls",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribePrefixLists",
        "ec2:DescribeRouteTables",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSpotInstanceRequests",
        "ec2:DescribeSpotPriceHistory",
        "ec2:DescribeSubnets",
        "ec2:DescribeTags",
        "ec2:DescribeVpcAttribute",
        "ec2:DescribeVpcEndpoints",
        "ec2:DescribeVpcEndpointServices",
        "ec2:DescribeVpcs",
        "ec2:DetachNetworkInterface",
        "ec2:ModifyImageAttribute",
        "ec2:ModifyInstanceAttribute",
        "ec2:RequestSpotInstances",
        "ec2:RevokeSecurityGroupEgress",
        "ec2:RunInstances",
        "ec2:TerminateInstances",
        "ec2:DeleteVolume",
        "ec2:DescribeVolumeStatus",
        "ec2:DescribeVolumes",
        "ec2:DetachVolume",
        "iam:GetRole",
        "iam:GetRolePolicy",
        "iam:ListInstanceProfiles",
        "iam:ListRolePolicies",
        "iam:PassRole",
        "cloudwatch:PutMetricAlarm",
        "cloudwatch:DescribeAlarms",
        "cloudwatch:DeleteAlarms",
        "application-autoscaling:RegisterScalableTarget",
        "application-autoscaling:DeregisterScalableTarget",
        "application-autoscaling:PutScalingPolicy",
        "application-autoscaling:DeleteScalingPolicy",
        "application-autoscaling:Describe*"
       ]
       resources = [ "*" ]
    }

    statement {
    effect = "Allow"
    actions = [ 
        "s3:Get*",
        "s3:List*"
     ]
    resources = [ "${aws_s3_bucket.s3.arn}","${aws_s3_bucket.s3.arn}/*" ]
  }
}

resource "aws_iam_role" "emr_service_role" {
  name = "demo-emr-service-role"
  assume_role_policy = data.aws_iam_policy_document.emr_service_role_assume.json
  inline_policy {
    name = "demo-emr-service-role-policy"
    policy = data.aws_iam_policy_document.emr_service_role_policy.json
  }
}

#EMR instance profile role
data "aws_iam_policy_document" "emr_profile_assume" {
  statement {
    actions = [ "sts:AssumeRole" ]
    effect = "Allow"
    principals {
      type = "Service"
      identifiers = [ "ec2.amazonaws.com" ]
    }
  }
}

data "aws_iam_policy_document" "emr_profile_policy" {
  statement {
    effect = "Allow"
    actions = [ 
        "cloudwatch:*",
        "ec2:Describe*",
        "elasticmapreduce:Describe*",
        "elasticmapreduce:ListBootstrapActions",
        "elasticmapreduce:ListClusters",
        "elasticmapreduce:ListInstanceGroups",
        "elasticmapreduce:ListInstances",
        "elasticmapreduce:ListSteps",
        "kinesis:CreateStream",
        "kinesis:DeleteStream",
        "kinesis:DescribeStream",
        "kinesis:GetRecords",
        "kinesis:GetShardIterator",
        "kinesis:MergeShards",
        "kinesis:PutRecord",
        "kinesis:SplitShard",
        "glue:CreateDatabase",
        "glue:UpdateDatabase",
        "glue:DeleteDatabase",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetTableVersions",
        "glue:CreatePartition",
        "glue:BatchCreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition",
        "glue:BatchDeletePartition",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchGetPartition",
        "glue:CreateUserDefinedFunction",
        "glue:UpdateUserDefinedFunction",
        "glue:DeleteUserDefinedFunction",
        "glue:GetUserDefinedFunction",
        "glue:GetUserDefinedFunctions"
     ]
     resources = [ "*" ]
  }

  statement {
    effect = "Allow"
    actions = [ 
        "s3:Get*",
        "s3:List*",
        "s3:Put*",
        "s3:DeleteObject"
     ]
    resources = [ "${aws_s3_bucket.s3.arn}","${aws_s3_bucket.s3.arn}/*" ]
  }
}

resource "aws_iam_role" "emr_profile_role" {
  name = "demo-EMR-EC2-InstanceProfile"
  assume_role_policy = data.aws_iam_policy_document.emr_profile_assume.json
  inline_policy {
    name = "demo-EMR-EC2-InstanceProfile-policy"
    policy = data.aws_iam_policy_document.emr_profile_policy.json
  }
}

resource "aws_iam_instance_profile" "emr_instance_profile" {
  name = "demo-EMR-EC2-InstanceProfile"
  role = aws_iam_role.emr_profile_role.name
}

#step function role
data "aws_iam_policy_document" "sfn_role_assume" {
  statement {
    actions = [ "sts:AssumeRole" ]
    effect = "Allow"
    principals {
      type = "Service"
      identifiers = [ "states.amazonaws.com" ]
    }
  }
}

data "aws_iam_policy_document" "sfn_role_policy" {
  statement {
    effect = "Allow"
    actions = [ 
        "events:PutTargets",
        "events:PutRule",
        "events:DescribeRule"
     ]
     resources = [ "arn:aws:events:ap-southeast-1:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventForEMRAddJobFlowStepsRule" ]
  }

  statement {
    effect = "Allow"
    actions = [ 
        "elasticmapreduce:AddJobFlowSteps",
        "elasticmapreduce:DescribeStep",
        "elasticmapreduce:CancelSteps",
        "elasticmapreduce:RunJobFlow",
        "elasticmapreduce:DescribeCluster",
        "elasticmapreduce:TerminateJobFlows"
     ]
    resources = [ "arn:aws:elasticmapreduce:ap-southeast-1:${data.aws_caller_identity.current.account_id}:cluster/*" ]
  }

  statement {
    effect = "Allow"
    actions = [ "iam:PassRole" ]
    resources = [ "${aws_iam_role.emr_service_role.arn}",
                  "${aws_iam_role.emr_profile_role.arn}",]
  }

  depends_on = [ aws_iam_role.emr_service_role, aws_iam_role.emr_profile_role ]
}

resource "aws_iam_role" "sfn_role" {
  name = "demo-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_role_assume.json
  inline_policy {
    name = "demo-sfn-role-policy"
    policy = data.aws_iam_policy_document.sfn_role_policy.json
  }
}

#lambda function execution role
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = [ "sts:AssumeRole" ]
    effect = "Allow"
    principals {
      type = "Service"
      identifiers = [ "lambda.amazonaws.com" ]
    }
  }
}

data "aws_iam_policy_document" "lambda_policy" {
  statement {
    effect = "Allow"
    actions = [ 
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
     ]
    resources = [ "${aws_cloudwatch_log_group.lambda_log.arn}:*" ] 
  }

  statement {
    effect = "Allow"
    actions = [ 
      "states:StartExecution"
     ]
    resources = [ "${aws_sfn_state_machine.sfn_emr.arn}" ]
  }
  depends_on = [ aws_cloudwatch_log_group.lambda_log ]
}

resource "aws_iam_role" "lambda_execution_role" {
  name = "demo-lambda-execution-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
  inline_policy {
    name = "demo-lambda-execution-policy"
    policy = data.aws_iam_policy_document.lambda_policy.json
  }
}