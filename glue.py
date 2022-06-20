import sys
import time
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from redshift import Redshift
from etl import ETL
import boto3
import json


region_name = "cn-northwest-1"

# ===========================需要修改的配置参数================================
redshift_secret_name = 'dev/demo/redshift'
secret_arn = 'arn:aws-cn:secretsmanager:cn-northwest-1:027040934161:secret:dev/demo/redshift-dpZ9yS'
redshift_db = 'dev'
redshift_temp_s3 = 's3://sam-data-platform-redshift-temp'
redshift_endpoint = "https://vpce-0c20d267bb10f74e6-zlv5xrlp.redshift-data.cn-northwest-1.vpce.amazonaws.com.cn"
dynamodb_sam_data_trace_tb = 'sam-data-upload-event'
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/sam-data-plaftform-test"
sqs_endpoint_url = "https://vpce-05ddb236d804f224d-s18hyk87.sqs.cn-northwest-1.vpce.amazonaws.com.cn"
# =============================上面参数需要修改==========================================

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def get_tasks():
    sqs = boto3.client('sqs', region_name=region_name,
                       endpoint_url=sqs_endpoint_url)
    dynamodb = boto3.client('dynamodb', region_name=region_name)

    index = 0
    while True:
        response = sqs.receive_message(
            QueueUrl=sqs_url,
            VisibilityTimeout=600
        )
        print(response)
        if "Messages" not in response:
            print(f"===> got {index}")
            break

        index += 1
        messages = response["Messages"]
        task_info = list()
        for msg in messages:
            body_str = msg["Body"]
            body = json.loads(body_str)
            receipt_handle = msg["ReceiptHandle"]
            sqs.delete_message(
                QueueUrl=sqs_url,
                ReceiptHandle=receipt_handle
            )
            task_info.append(body)

        for body in task_info:
            #  "file_key": file_key,
            #  "event_time": current_time,
            #  "status": "begin"

            # 修改状态为正在开始处理
            dynamodb.update_item(
                TableName=dynamodb_sam_data_trace_tb,
                Key={
                    'file_key': {'S': body['file_key']},
                    'event_time':  {'S': body['event_time']}

                },
                AttributeUpdates={
                    'status': {
                        'Value':  {
                            "S": "processing"
                        }
                    },
                    'msg': {
                        'Value':  {
                            "S": "etl"
                        }
                    }
                }
            )

            info = yield body
            parts = info.split("==>")
            task_status = parts[0]
            msg = parts[1]
            # 修改状态为正在完成或失败
            dynamodb.update_item(
                TableName=dynamodb_sam_data_trace_tb,
                Key={
                    'file_key': {'S': body['file_key']},
                    'event_time':  {'S': body['event_time']}

                },
                AttributeUpdates={
                    'status': {
                        'Value':  {
                            "S": task_status
                        }
                    },
                    'msg': {
                        'Value':  {
                            "S": msg
                        }
                    }
                }
            )
    yield None


def write_data(redshift: Redshift, target_tb: str, history_tb_name: str, df_data: DataFrame, data_date: str, data_version: str):
    current_date = datetime.now().strftime("%Y-%m-%d")
    # 保存所有历史的数据，历史表添加一个数据etl 日期
    df_data_history = df_data.withColumn("etl_date", F.lit(current_date))
    df_data_history = df_data_history.withColumn(
        "etl_version",  F.lit(data_version))
    df_data_history = df_data_history.withColumn(
        "file_upload_date", F.lit(data_date))

    dyn_df_data_history = DynamicFrame.fromDF(
        df_data_history, glueContext, "nested")

    # write to history table
    print(f"begin to write history table {history_tb_name}")
    append_options = redshift.conn_option(history_tb_name)
    glueContext.write_dynamic_frame.from_options(
        frame=dyn_df_data_history,
        connection_type="redshift",
        connection_options=append_options
    )

    # write to current table
    print(f"begin to write stable table {target_tb}")
    dyn_df_data = DynamicFrame.fromDF(df_data, glueContext, "nested")
    overwrite_options = redshift.conn_option(target_tb, "overwrite")

    glueContext.write_dynamic_frame.from_options(
        frame=dyn_df_data,
        connection_type="redshift",
        connection_options=overwrite_options
    )


def main():
    w = write_data
    gen = get_tasks()
    task = next(gen)
    worker = ETL(spark, secret_arn, redshift_secret_name, redshift_db,
                 redshift_endpoint, redshift_temp_s3)
    while task:
        file_path = task['file_key']
        print(f"=========> got task {file_path}")
        try:
            worker.do_task(file_path, w_data=w)
            task = gen.send("successful==>successful")
        except Exception as e:
            print(f"ERROR ===========> {e}")
            task = gen.send(f"failed==>{e}")
        time.sleep(1)


main()
