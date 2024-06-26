import json
import boto3
import configparser

#get config
config = configparser.ConfigParser()
config.read('init.ini')

#get step function arn
step_function_arn = config['step_functions']['step_functions_arn']

def lambda_handler(event, context):
    #S3 notification
    print(event)
    
    #get input url
    s3_data = event['Records'][0]['s3']
    bucket_name = s3_data['bucket']['name']
    file_name = s3_data['object']['key']
    file_url = f's3://{bucket_name}/{file_name}'
    print(f'file url: {file_url}')

    #start step functions execution
    sfn_client = boto3.client('stepfunctions')
    response = sfn_client.start_execution(
        stateMachineArn = step_function_arn,
        input = json.dumps({'source_url': file_url})
    )
    print(response)


