import json
import time
import urllib.parse
import boto3
from datetime import datetime, timedelta

region_name = "cn-northwest-1"
max_parallel_count = 5
time_diff = 200  # 十分钟内不容许有相同的文件上传

# ===========================需要修改或确认的配置参数================================
dynamodb_sam_data_trace_tb = 'sam-data-upload-event'
workflow_name = "sam-data-workflow"
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/sam-data-plaftform-test"
# ===========================需要修改或确认的配置参数================================

compare = (datetime.now() - timedelta(minutes=time_diff)
           ) .strftime("%Y-%m-%d %H:%M:%S")
print(compare)
dynamodb = boto3.client('dynamodb', region_name=region_name)
file_key = 's3://tx-sam-data-files/2022-06-06/XiaoShouYi_Partner - Contact_001.csv'
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
                    'S': compare
                },
            ],
            'ComparisonOperator': 'GT'
        }
    })
print("=========")
print(len(response['Items']))
result = list()
for item in response['Items']:
    d = dict()
    d['event_time'] = item['event_time']['S']
    d['file_key'] = item['file_key']['S']
    d['status'] = item['status']['S']
    result.append(d)

print(result)
