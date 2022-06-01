
import time
from datetime import datetime
import boto3
import json

region_name = "cn-northwest-1"
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/sam-data-plaftform-test"
sqs_endpoint_url = "https://vpce-05ddb236d804f224d-s18hyk87.sqs.cn-northwest-1.vpce.amazonaws.com.cn"


def get_task():
    sqs = boto3.client('sqs', region_name=region_name,
                       endpoint_url=sqs_endpoint_url)

    index = 0
    while True:
        response = sqs.receive_message(
            QueueUrl=sqs_url,
            VisibilityTimeout=300
        )
        if "Messages" not in response:
            print(f"===> got {index}")
            break

        index += 1
        messages = response["Messages"]
        for msg in messages:
            body_str = msg["Body"]
            body = json.loads(body_str)
            receipt_handle = msg["ReceiptHandle"]
            yield body
            sqs.delete_message(
                QueueUrl=sqs_url,
                ReceiptHandle=receipt_handle
            )


for task in get_task():
    print(task)
    time.sleep(1)
