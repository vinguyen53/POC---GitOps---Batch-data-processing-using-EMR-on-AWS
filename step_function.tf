#Create step function
resource "aws_sfn_state_machine" "sfn_emr" {
  name = "demo-sfn-emr"
  role_arn = aws_iam_role.sfn_role.arn
  definition = <<EOF
    {
  "Comment": "An example of the Amazon States Language for running jobs on Amazon EMR",
  "StartAt": "Create an EMR cluster",
  "States": {
    "Create an EMR cluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:createCluster.sync",
      "Parameters": {
        "Name": "demo-sfn-emr-cluster",
        "VisibleToAllUsers": true,
        "ReleaseLabel": "emr-7.1.0",
        "Applications": [
          {
            "Name": "Hadoop"
          },
          {
            "Name": "Spark"
          },
          {
            "Name": "Hive"
          }
        ],
        "ServiceRole": "${aws_iam_role.emr_service_role.name}",
        "JobFlowRole": "${aws_iam_role.emr_profile_role.name}",
        "LogUri": "s3://ntanvi-sfn-emr-demo/logs",
        "Instances": {
          "KeepJobFlowAliveWhenNoSteps": true,
          "AdditionalMasterSecurityGroups": ["${aws_security_group.emr-master-sg.id}"],
          "Ec2SubnetId": "${aws_subnet.emr-subnet.id}",
          "InstanceGroups": [
            {
              "InstanceCount": 1,
              "InstanceRole": "MASTER",
              "InstanceType": "m5.xlarge"
            },
            {
              "InstanceCount": 1,
              "InstanceRole": "CORE",
              "InstanceType": "m5.xlarge"
            }
          ]
        },
        "Configurations": [
          {
            "Classification": "hive-site",
            "Properties": {
              "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
          },
          {
            "Classification": "spark-hive-site",
            "Properties": {
              "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
          },
          {
            "Classification": "iceberg-defaults",
            "Properties": {
              "iceberg.enabled": "true"
            }
          }
        ]
      },
      "ResultPath": "$.cluster",
      "Next": "Run first step"
    },
    "Run first step": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:addStep.sync",
      "Parameters": {
        "ClusterId.$": "$.cluster.ClusterId",
        "Step": {
          "Name": "Run spark job",
          "ActionOnFailure": "CONTINUE",
          "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args.$": "States.Array('spark-submit','--deploy-mode','cluster','s3://ntanvi-sfn-emr-demo/scripts/pyspark_iceberg.py','--input_url',$.source_url)"
          }
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "ResultPath": "$.firstStep",
      "Next": "Terminate Cluster"
    },
    "Terminate Cluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:terminateCluster",
      "Parameters": {
        "ClusterId.$": "$.cluster.ClusterId"
      },
      "End": true
    }
  }
}
  EOF
}