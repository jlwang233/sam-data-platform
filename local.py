import random
from flask import redirect
from numpy import column_stack
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, \
    IntegerType, TimestampType, DoubleType, LongType
import pandas as pd
import json
import os
import boto3
from redshift import Redshift
from column_helper import ColumnDataTypeCheck, ColumnFormater
import time

# TODO 修改成您的 secret_name， region_name
region_name = "cn-northwest-1"

redshift_secret_name = 'dev/demo/redshift'
secret_arn = 'arn:aws-cn:secretsmanager:cn-northwest-1:027040934161:secret:dev/demo/redshift-dpZ9yS'
redshift_db = 'dev'


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

    df_header = session.createDataFrame(header)
    df_data = session.createDataFrame(data, df_scheme)
    return df_header, df_data


def dtype_from_redshift_2_pyspark(dtype: str) -> str:
    if dtype == "bigint":
        return "long"
    if dtype == "int":
        return "int"
    if dtype == "real":
        return "double"
    return "string"


def get_data(spark: SparkSession, file_name) -> DataFrame:
    folder = os.getcwd()
    file_path = f'{folder}/sample-data/{file_name}'

    df_raw = spark.read.option(
        "multiLine", "true"
    ).option(
        "header", "true"
    ).option(
        "encoding", 'GBK'
    ).csv(file_path)
    return df_raw


def write_data(spark: SparkSession, df: DataFrame):
    pass


def create_table(redshift: Redshift, tb_name: str, df_header: DataFrame, df_data: DataFrame):
    column_checker = ColumnDataTypeCheck()
    column_with_dtype = column_checker.run(df_data)
    btsql = redshift.create(tb_name, df_data.columns, column_with_dtype)

    ok = redshift.exec(btsql)
    if not ok:
        print("create table on redshift failed")
        return

    # 添加注释
    comments = get_column_chinese(df_header)
    redshift.comments(tb_name, comments)


def add_new_columns_to_schema(redshift: Redshift, tb_name: str, df_data: DataFrame, redshift_schema: dict):
    """
    根据数据判断一下新的数据里面是否有新增列，有的话，则需要修改redshift 的表schema
    """
    data_columns = df_data.columns
    # 有新增列，则修改redshift 表结构，添加新列
    new_columns = [c for c in data_columns if c not in redshift_schema]

    # 如果列名变更了，这么没法判断，所以就当初即新增，又删除
    if len(new_columns) > 0:
        column_checker = ColumnDataTypeCheck()
        # 要通过采样，获取新增列的数据类型
        column_with_dtype = column_checker.run(df_data, new_columns)
        redshift.add_columns(tb_name, column_with_dtype)


def fill_lost_columns(schema: dict, df_data: DataFrame):
    # 有删除列，则要在数据中添加删除的列，值设置为空
    deleted_columns = [c for c in schema if c not in df_data.columns]

    if len(deleted_columns) > 0:
        for column in deleted_columns:
            dtype = schema[column]
            pyspark_dtype = dtype_from_redshift_2_pyspark(dtype)
            df_data = df_data.withColumn(
                column, F.lit(None).cast(pyspark_dtype))
    return df_data


def main():
    # 表明从dynamodb 去获取
    file_name = 'XiaoShouYi_Customer_001.csv'

    parts = file_name.split("_")
    uparts = parts[0: len(parts) - 1]
    target_table_name = "_".join(uparts).lower()
    print(target_table_name)

    spark = SparkSession.builder.getOrCreate()

    df_raw = get_data(spark, file_name)

    df_raw = df_raw.drop("English")
    df_header, df_data = split_df_header(spark, df_raw)

    formater = ColumnFormater()
    df_header = formater.run(df_header)
    df_data = formater.run(df_data)

    redshift = Redshift(region_name, secret_arn,
                        redshift_secret_name, redshift_db)
    schema = redshift.schema(target_table_name)

    if not schema:
        # 如果redshift上还没有有这个类别的表，则在redshift建表
        # 手动建表的原因：
        # 1. 原始数据比较乱，pyspark 推测不出数据，都默认为string了
        # 2. 建表后，需要手动添加每个字段的注释，作为字段别名
        create_table(redshift, target_table_name, df_header, df_data)
    else:
        # 有新增列，则修改redshift 表结构，添加新列
        add_new_columns_to_schema(redshift, target_table_name, df_data, schema)

        # 有删除列，则要在数据中添加删除的列，值设置为空
        df_data = fill_lost_columns(schema, df_data)


main()
