import re
import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType, DoubleType, LongType
from redshift import Redshift
from etl import ETL
from column_helper import ColumnDataTypeCheck, ColumnFormater
import time


# TODO 修改成您的 secret_name， region_name
region_name = "cn-northwest-1"

redshift_secret_name = 'dev/demo/redshift'
secret_arn = 'arn:aws-cn:secretsmanager:cn-northwest-1:027040934161:secret:dev/demo/redshift-dpZ9yS'
redshift_db = 'dev'
redshift_temp_s3 = 's3://txt-glue-code'
redshift_endpoint = "https://vpce-0c20d267bb10f74e6-zlv5xrlp.redshift-data.cn-northwest-1.vpce.amazonaws.com.cn"
file_s3_path = '/home/ec2-user/code/sam-data-platform/sample-data/2022-06-01/MP report_v001.csv'
# file_s3_path = '/home/ec2-user/code/sam-data-platform/sample-data/XiaoShouYi_Customer_001.csv'

spark = SparkSession.builder.getOrCreate()


def write_data(redshift: Redshift, target_tb: str, history_tb_name: str, df_data: DataFrame, data_date: str, data_version: str):
    df_data.write.csv(
        f"/home/ec2-user/code/sam-data-platform/output/{target_tb}.csv")
    pass


def get_tasks():
    yield {'file_key': file_s3_path}
    yield None


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
