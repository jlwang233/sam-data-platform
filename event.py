import json
import time
import urllib.parse
import boto3
from datetime import datetime

region_name = "cn-northwest-1"
max_parallel_count = 10

# ===========================需要修改或确认的配置参数================================
dynamodb_sam_data_trace_tb = 'sam-data-upload-event'
workflow_name = "sam-data-workflow"
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/sam-data-plaftform-test"
# ===========================需要修改或确认的配置参数================================


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    keys = [record['s3']['bucket']['name'] + "/" + record['s3']['object']['key']
            for record in event['Records']]
    print(keys)
    try:
        dynamodb = boto3.client('dynamodb', region_name=region_name)
        sqs = boto3.client('sqs', region_name=region_name)
        for key in keys:
            ukey = urllib.parse.unquote_plus(key, encoding='utf-8')
            file_key = "s3://" + ukey
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

        glue = boto3.client('glue', region_name=region_name)
        # for i in range(0, parallel_count):
        #     glue.start_workflow_run(
        #         Name=workflow_name,
        #     )
        #     time.sleep(0.5)
        return {
            "status": "ok"
        }
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
