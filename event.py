import json
import urllib.parse
import boto3
from datetime import datetime

region_name = "cn-northwest-1"
dynamodb_sam_data_trace_tb = 'sam-data-upload-event'

s3 = boto3.client('s3')
glue = boto3.client('glue')


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    keys = [record['s3']['bucket']['name'] + "/" + record['s3']['object']['key']
            for record in event['Records']]

    session = boto3.Session()
    dynamodb = session.client('dynamodb', region_name=region_name)

    for key in keys:
        dynamodb.update_item(
            TableName=dynamodb_sam_data_trace_tb,
            Key={
                'file_key': {'S': "s3://" + key},
                'event_time':  {'S': current_time}

            },
            AttributeUpdates={
                'status': {
                    'Value':  {
                        "S": "begin"
                    }
                }
            }
        )

    # Get the object from the event and show its content type

    try:
        response = glue.start_workflow_run(
            Name='xiaoshouyi',
        )
        return response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
