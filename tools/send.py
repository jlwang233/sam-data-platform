import sys
import time

from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, DateType
from redshift import Redshift
from column_helper import ColumnDataTypeCheck, ColumnFormater
import boto3
import json


region_name = "cn-northwest-1"
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/sam-data-plaftform-test"
sqs_endpoint_url = "https://vpce-05ddb236d804f224d-s18hyk87.sqs.cn-northwest-1.vpce.amazonaws.com.cn"


sqs = boto3.client('sqs', region_name=region_name,
                   endpoint_url=sqs_endpoint_url)

for i in range(0, 10):
    k_info = {
        "file_key": str(i),
        "event_time": str(i),
        "status": "begin"
    }
    str_info = json.dumps(k_info)
    sqs.send_message(
        QueueUrl=sqs_url,
        MessageBody=str_info,
        DelaySeconds=0
    )
time.sleep(1)
