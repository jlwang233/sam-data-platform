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
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, DateType
from redshift import Redshift
from column_helper import ColumnDataTypeCheck, ColumnFormater
import boto3
import json
import re


region_name = "cn-northwest-1"

# ===========================需要修改的配置参数================================
redshift_secret_name = 'dev/sam/redshift'
secret_arn = 'arn:aws-cn:secretsmanager:cn-northwest-1:079001511566:secret:dev/sam/redshift-tctyOg'
redshift_db = 'dev'
redshift_temp_s3 = 's3://xy-sam-data-platform-redshift-temp'
redshift_endpoint = "https://vpce-09e7e080970b5aafc-vx81ozci.redshift-data.cn-northwest-1.vpce.amazonaws.com.cn"
dynamodb_sam_data_trace_tb = 'xy-sam-data-upload-event'
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/079001511566/xy-sam-data-sqs"
sqs_endpoint_url = "https://vpce-00d7a33aec3f90717-5lz7g217.sqs.cn-northwest-1.vpce.amazonaws.com.cn"
# =============================上面参数需要修改==========================================

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def get_column_chinese(df: DataFrame) -> dict:
    rows = df.collect()
    column_key_name_dict = dict()
    for item in zip(df.columns, rows[2]):
        column_key_name_dict[item[0]] = item[1]
    return column_key_name_dict


def split_df_header(session: SparkSession, df: DataFrame):

    rows = df.collect()
    header = rows[0:3]
    data = rows[3:]

    field_schema = [StructField(column, StringType()) for column in df.columns]

    df_scheme = StructType(field_schema)

    df_header = session.createDataFrame(header, df_scheme)
    df_data = session.createDataFrame(data, df_scheme)
    return df_header, df_data


def add_new_columns_to_schema(redshift: Redshift, tb_name: str, df_data: DataFrame, redshift_schema: dict):
    """
    根据数据判断一下新的数据里面是否有新增列，有的话，则需要修改redshift 的表schema
    根据采样数据的类型，更新这些新增列的数据类型
    """
    data_columns = df_data.columns
    # 有新增列，则修改redshift 表结构，添加新列
    new_columns = [c for c in data_columns if c not in redshift_schema]

    if len(new_columns) > 0:
        print(f"=========> got new columns {new_columns}")
        column_checker = ColumnDataTypeCheck()
        # 要通过采样，获取新增列的数据类型
        column_with_dtype = column_checker.run(df_data, new_columns)
        redshift.add_columns(tb_name, column_with_dtype)

        # 新增的列需要通过采样更新数据类型
        for column in column_with_dtype:
            dtype = column_with_dtype[column]
            d = ColumnDataTypeCheck.to_spark_dtype(dtype)
            df_data = df_data.withColumn(column, df_data[column].cast(d))
    return df_data


def fill_lost_columns(schema: dict, df_data: DataFrame):
    # 有删除列，则要在数据中添加删除的列，值设置为空
    deleted_columns = [c for c in schema if c not in df_data.columns]

    if len(deleted_columns) > 0:
        for column in deleted_columns:
            dtype = schema[column]
            d = ColumnDataTypeCheck.to_spark_dtype(dtype)
            df_data = df_data.withColumn(column, F.lit(None).cast(d))
    return df_data


def update_column_dtype(redshift_schema: dict, df_data: DataFrame):
    for column in df_data.columns:
        if column not in redshift_schema:
            continue

        dtype = redshift_schema[column]
        d = ColumnDataTypeCheck.to_spark_dtype(dtype)
        df_data = df_data.withColumn(column, df_data[column].cast(d))
    return df_data


reg_ex = re.compile('[^A-Za-z0-9]')


def get_tb_name(s3_path: str):
    path_parts = s3_path.split("/")
    count = len(path_parts)

    file_name = path_parts[count-1]

    parts = reg_ex.split(file_name)
    used_parts = [item for item in parts if item.strip() != '']
    ll = len(used_parts)
    version = used_parts[ll-2]
    tb_name = "_".join(used_parts[:ll-2]).lower()

    file_date = path_parts[count-2]
    return tb_name, file_date, version


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


def get_data(spark: SparkSession, s3_path: str) -> DataFrame:

    df_raw = spark.read.option(
        "multiLine", "true"
    ).option(
        "header", "true"
    ).option(
        "encoding", 'GBK'
    ).csv(s3_path)
    df_raw = df_raw.drop("English")

    return df_raw


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


def do_task(file_s3_path: str):
    # 表明从dynamodb 去获取
    target_tb, tb_date, version = get_tb_name(file_s3_path)

    df_raw = get_data(spark, file_s3_path)
    df_raw.printSchema()

    print("===============> begin format")
    formater = ColumnFormater()
    df_raw = formater.remove_unused_columns(df_raw)

    df_header, df_data = split_df_header(spark, df_raw)

    df_header = formater.run(df_header)
    df_data = formater.run(df_data)
    print("===============> end format")
    redshift = Redshift(region_name, secret_arn,
                        redshift_secret_name, redshift_db, redshift_endpoint, redshift_temp_s3)
    schema = redshift.schema(target_tb)
    print("===============> check schema")
    df_data.printSchema()

    # 默认情况，由于有大量缺失值，数据类型都是string
    if not schema:
        # 如果没有在 db 创建过该表，则需要根据数据采样，来修正数据的类型信息
        column_checker = ColumnDataTypeCheck()
        column_with_dtype = column_checker.run(df_data)
        df_data = update_column_dtype(column_with_dtype, df_data)
        print("===============> update schema from sampling")

    else:
        print("=============> schema exist in redshift")
        print(schema)
        # 如果db 中已经存在该表，则根据数据库中的表schema，更新数据的schema
        df_data = update_column_dtype(schema, df_data)
        # 有新增列，则修改redshift 表结构，添加新列,并根据采样修正数据中新增列的数据类型
        df_data = add_new_columns_to_schema(
            redshift, target_tb, df_data, schema)

        # 有删除列，则要在数据中添加删除的列，值设置为空
        df_data = fill_lost_columns(schema, df_data)
        print("===============> update schema from redshift")

    df_data.printSchema()

    history_tb_name = target_tb + "_history"

    # 写入数据
    write_data(redshift, target_tb, history_tb_name, df_data, tb_date, version)

    # 如果是第一次写入数据后，则需要添加注释信息
    if not schema:
        time.sleep(1)
        comments = get_column_chinese(df_header)
        redshift.comments(target_tb, comments)
        redshift.comments(history_tb_name, comments)


def main():
    gen = get_tasks()
    task = next(gen)
    while task:
        file_path = task['file_key']
        print(f"=========> got task {file_path}")
        try:
            do_task(file_path)
            task = gen.send("successful==>successful")
        except Exception as e:
            print(f"ERROR ===========> {e}")
            task = gen.send(f"failed==>{e}")
        time.sleep(1)

    job.commit()


main()