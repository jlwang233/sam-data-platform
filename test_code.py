import re
import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType
from redshift import Redshift
from column_helper import ColumnDataTypeCheck, ColumnFormater
import time


@F.udf(returnType=IntegerType())
def str_to_int(numStr):
    l = len(numStr)
    if l > 3:
        numStr = numStr.replace(',', '')
    return int(numStr)


file_s3_path = '/home/ec2-user/code/sam-data-platform/sample-data/2022-06-01/NWCD Credit Report_ 2022 April.csv'
spark = SparkSession.builder.getOrCreate()


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


df = get_data(spark, file_s3_path)
df.printSchema()

df = df.select("credit_redeemed_usd_amount")
df = df.withColumn("credit_redeemed_usd_amount", str_to_int(
                   df["credit_redeemed_usd_amount"]))

df.show(100)
