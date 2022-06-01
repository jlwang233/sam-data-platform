import json
import time
import urllib.parse
import boto3
from datetime import datetime

region_name = "cn-northwest-1"
dynamodb_sam_data_trace_tb = 'sam-data-upload-event'
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/sam-data-plaftform-test"
workflow_name = "xiaoshouyi"
max_parallel_count = 10


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    s3 = boto3.client('s3', region_name=region_name)
    glue = boto3.client('glue', region_name=region_name)

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    keys = [record['s3']['bucket']['name'] + "/" + record['s3']['object']['key']
            for record in event['Records']]

    try:
        dynamodb = boto3.client('dynamodb', region_name=region_name)
        sqs = boto3.client('sqs', region_name=region_name)
        for key in keys:
            file_key = "s3://" + key
            dynamodb.update_item(
                TableName=dynamodb_sam_data_trace_tb,
                Key={
                    'file_key': {'S': file_key},
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
            k_info = {
                "file_key": file_key,
                "event_time": current_time,
                "status": "begin"
            }

            str_info = json.dumps(k_info)

            sqs.send_message(
                QueueUrl=sqs_url,
                MessageBody=str_info,
                DelaySeconds=0
            )

        parallel_count = min(max_parallel_count, len(keys))
        for i in range(0, parallel_count):
            glue.start_workflow_run(
                Name=workflow_name,
            )
            time.sleep(1)
        return {
            "status": "ok"
        }
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
