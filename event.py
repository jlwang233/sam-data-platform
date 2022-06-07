import json
import time
import urllib.parse
import boto3
from datetime import datetime, timedelta

region_name = "cn-northwest-1"
max_parallel_count = 5
time_diff = 10  # 十分钟内不容许上传相同的文档

# ===========================需要修改或确认的配置参数================================
dynamodb_sam_data_trace_tb = 'sam-data-upload-event'
workflow_name = "sam-data-workflow"
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/sam-data-plaftform-test"
# ===========================需要修改或确认的配置参数================================


def check_key(dynamodb, file_key, cond):

    response = dynamodb.query(
        TableName=dynamodb_sam_data_trace_tb,
        Limit=100,
        Select='ALL_ATTRIBUTES',
        KeyConditions={
            'file_key': {
                'AttributeValueList': [
                    {
                        'S': file_key,
                    },
                ],
                'ComparisonOperator': 'EQ'
            },
            'event_time': {
                'AttributeValueList': [
                    {
                        'S': cond
                    },
                ],
                'ComparisonOperator': 'GT'
            }
        })
    return len(response['Items'])


def lambda_handler(event, context):
    print("Received event: " + str(event))
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    condition = (now - timedelta(minutes=time_diff)
                 ).strftime("%Y-%m-%d %H:%M:%S")

    keys = [record['s3']['bucket']['name'] + "/" + record['s3']['object']['key']
            for record in event['Records']]
    task_count = 0
    try:
        dynamodb = boto3.client('dynamodb', region_name=region_name)
        sqs = boto3.client('sqs', region_name=region_name)
        for key in keys:
            ukey = urllib.parse.unquote_plus(key, encoding='utf-8')
            file_key = "s3://" + ukey
            c = check_key(dynamodb, file_key, condition)
            # 已经存在了
            if c > 0:
                continue

            task_count += 1
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

        parallel_count = min(max_parallel_count, task_count)

        glue = boto3.client('glue', region_name=region_name)
        for i in range(0, parallel_count):
            p = glue.start_workflow_run(
                Name=workflow_name,
            )
            print(f"got glue workflow {p}")
    except Exception as e:
        print(e)
        return {
            "status": str(e)
        }

    return {
        "status": "ok"
    }
