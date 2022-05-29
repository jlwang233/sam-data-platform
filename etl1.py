import random
from numpy import column_stack
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, \
    IntegerType, TimestampType, DoubleType, LongType
from datetime import datetime, date
import pandas as pd
import json
import os
import re
import boto3


reg = "[^0-9A-Za-z_]"
bool_char = ['y', 'n', "true", "false"]
datetime1_format = "%Y-%m-%d %H:%M:%S"
datetime2_format = "%Y-%m-%d %H:%M"
date_format = "%Y-%m-%d"


def is_float(item: str) -> bool:
    try:
        float(item)
        return True
    except:
        return False


def is_int(item: str) -> bool:
    try:
        int(item)
        return True
    except:
        return False


def is_bool(item: str) -> bool:
    return item.lower() in bool_char


def is_datetime(item: str) -> bool:

    try:
        res = bool(datetime.strptime(item, datetime1_format))
        return True
    except ValueError:
        res = False

    try:
        res = bool(datetime.strptime(item, datetime2_format))
        return True
    except ValueError:
        res = False

    return res


def is_date(item: str) -> bool:
    try:
        res = bool(datetime.strptime(item, date_format))
    except ValueError:
        res = False

    return res


def update_duplicate_columns(df: DataFrame) -> DataFrame:
    duplicate_columns = dict()
    # 查找重复列
    column_names = df.columns
    column_l = len(column_names)
    for index in range(1, column_l):
        column_name = column_names[index]
        if column_name.endswith(str(index)):
            end_pos = len(column_name) - len(str(index))
            key = column_name[0: end_pos]
            if key in duplicate_columns:
                duplicate_columns[key].append(column_name)
            else:
                duplicate_columns[key] = [column_name]

    # 对于重复列，值不为空的那一列作为主列，其余为附属列
    for key in duplicate_columns:
        compare_columns = duplicate_columns[key]
        if len(compare_columns) > 1:
            big_one = None
            big_one_value = 0
            for column_name in compare_columns:

                u = df.filter(f"{column_name} is NOT NULL").count()
                if u > big_one_value:
                    big_one_value = u
                    big_one = column_name

            index_name = 1
            for column_name in compare_columns:
                if column_name == big_one:
                    df = df.withColumnRenamed(column_name, key)
                else:
                    new_name = f"{key}_old_{index_name}"
                    df = df.withColumnRenamed(column_name, new_name)
                    index_name += 1

    return df


def format_column_name(df: DataFrame) -> DataFrame:
    column_names = df.columns

    for column_name in column_names:
        new_name = column_name
        new_name = new_name.strip()
        new_name = new_name.replace(' ', '_')
        new_name = new_name.replace('-', '_')
        new_name = new_name.replace('__', '_')
        new_name = re.sub(reg, '', new_name)

        if column_name != new_name:
            df = df.withColumnRenamed(column_name, new_name)
    return df


# def remove_unknown_columns(df: DataFrame) -> DataFrame:
#     columns = df.columns
#     for column in columns:
#         if column.startswith("_c"):
#             df = df.drop(column)
#     return df


def guess_df_column_data_type(df: DataFrame) -> dict:
    result = dict()
    column_names = df.columns

    for column_name in column_names:

        dfu = df.select(column_name)

        dfu = dfu.filter(f'{column_name} is NOT NULL')
        dfsample = dfu.sample(0.1)

        if dfsample.count() == 0:
            rows = dfu.collect()
        else:
            rows = dfsample.collect()

        count = len(rows)
        max_count = 5
        check_count = min(count, max_count)

        intCount = 0
        floatCount = 0
        boolCount = 0
        datetimeCount = 0
        dateCount = 0
        print(f"{column_name}={check_count}")

        loop_index = 0
        for row in rows:
            if loop_index >= max_count:
                break
            loop_index += 1

            for v in row:
                print(v)
                if is_int(v):
                    intCount += 1
                    continue

                if is_float(v):
                    floatCount += 1
                    continue

                if is_bool(v):
                    boolCount += 1
                    continue

                if is_datetime(v):
                    datetimeCount += 1
                    continue

                if is_date(v):
                    dateCount += 1
                    continue

        result[column_name] = 'VARCHAR(500)'
        for item in [(intCount, "BIGINT"), (floatCount, "REAL"), (boolCount, "BOOLEAN"), (datetimeCount, "TIMESTAMP"), (dateCount, "DATE")]:
            print(item)
            if item[0] == check_count:
                result[column_name] = item[1]
                break

    return result


def guess_column_data_type(rows: list, columns: list) -> dict:
    stat = dict()
    for column in columns:
        # int, float, bool, datetime, date,string
        stat[column] = (0, 0, 0, 0, 0)
    samples = random.sample(rows, 10)
    for row in rows:
        int
        for column in columns:
            v = row[column]
            if is_int(v):
                intCount += 1
                continue

            if is_float(v):
                floatCount += 1
                continue

            if is_bool(v):
                boolCount += 1
                continue

            if is_datetime(v):
                datetimeCount += 1
                continue

            if is_date(v):
                dateCount += 1
                continue


def build_create_tb(tb_name: str, columns: list, schema: dict) -> str:
    sql_list = list()
    sql_list.append(f"CREATE TABLE IF NOT EXISTS {tb_name} (")

    # columns 保证列顺序一致
    index = 0
    column_l = len(columns)
    for column in columns:
        dtype = schema[column]
        if index == column_l:
            sql_list.append(f"{column} {dtype}")
        else:
            sql_list.append(f"{column} {dtype},")
        index += 1
    sql_list.append(");")
    return "\n".join(sql_list)


def split_df_header(session: SparkSession, df: DataFrame):

    rows = df.collect()
    header = rows[0:3]
    data = rows[3:]

    field_schema = [StructField(column, StringType()) for column in df.columns]

    df_scheme = StructType(field_schema)

    df_header = session.createDataFrame(header)
    df_data = session.createDataFrame(data, df_scheme)
    return df_header, df_data


if __name__ == '__main__':
    folder = os.getcwd()
    file_path = f'{folder}/sample-data/XiaoShouYi_Customer_001.csv'

    spark = SparkSession.builder.getOrCreate()

    df_raw = spark.read.option(
        "multiLine", "true"
    ).option(
        "header", "true"
    ).option(
        "encoding", 'GBK'
    ).csv(file_path)
    df_raw = df_raw.drop("English")
    df_header, df_data = split_df_header(spark, df_raw)

    # rows = df_raw.collect()
    # chinese = rows[2]
    # data = rows[3:]

    # column_key_name_dict = dict()

    # for item in zip(df_raw.columns, chinese):
    #     column_key_name_dict[item[0]] = item[1]

    # df = spark.createDataFrame(data)

    df_data = format_column_name(df_data)
    df_data = update_duplicate_columns(df_data)

    r = guess_df_column_data_type(df_data)
    df_data.printSchema()
    btsql = build_create_tb("customer", df_data.columns, r)
    print(btsql)
