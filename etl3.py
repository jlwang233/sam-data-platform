
from random import Random, random
from tokenize import Double
from numpy import column_stack, double
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, \
    IntegerType, TimestampType, DoubleType, LongType, BooleanType, TimestampType, DateType
from datetime import datetime, date
import pandas as pd
import json
import os
import re
import boto3


if __name__ == '__main__':
    folder = os.getcwd()
    file_path = f'{folder}/sample-data/XiaoShouYi_Customer_002.csv'

    spark = SparkSession.builder.getOrCreate()

    df_raw = spark.read.option(
        "multiLine", "true"
    ).option(
        "header", "true"
    ).option(
        "encoding", 'GBK'
    ).csv(file_path)
    df_raw = df_raw.drop("English")
    df_raw.printSchema()

    df_raw = df_raw.withColumn("test_null", F.lit(None).cast(LongType()))
    df_raw = df_raw.withColumn("test_null2", F.lit(None).cast("double"))
    df_raw = df_raw.withColumn("test_null3", F.lit(None).cast(TimestampType()))
    df_raw = df_raw.withColumn("test_null4", F.lit(None).cast(DateType()))
    # df_raw = df_raw.withColumn("test_null3", F.lit(None).cast(TimestampType()))
    # df_raw = df_raw.withColumn("test_null4", F.lit(None).cast(DoubleType()))

    df_raw.printSchema()

    df_raw.withColumn(
        "test_null", df_raw["test_null"].cast("boolean"))
    df_raw.printSchema()
