import re
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from redshift import Redshift
from column_helper import ColumnDataTypeCheck, ColumnFormater
import time

region_name = "cn-northwest-1"


class ETL:
    def __init__(self, spark: SparkSession,
                 secret_arn: str,
                 redshift_secret_name: str,
                 redshift_db: str,
                 redshift_endpoint: str,
                 redshift_temp_s3: str):

        self.spark = spark
        self.secret_arn = secret_arn
        self.redshift_db = redshift_db
        self.redshift_endpoint = redshift_endpoint
        self.redshift_temp_s3 = redshift_temp_s3
        self.redshift_secret_name = redshift_secret_name
        self.reg_ex = re.compile('[^A-Za-z0-9]')

    def get_column_chinese(self, df: DataFrame) -> dict:
        rows = df.collect()
        column_key_name_dict = dict()
        for item in zip(df.columns, rows[2]):
            column_key_name_dict[item[0]] = item[1]
        return column_key_name_dict

    def split_df_header(self, session: SparkSession, df: DataFrame):

        rows = df.collect()
        header = rows[0:3]
        data = rows[3:]

        field_schema = [StructField(column, StringType())
                        for column in df.columns]

        df_scheme = StructType(field_schema)

        df_header = session.createDataFrame(header, df_scheme)
        df_data = session.createDataFrame(data, df_scheme)
        return df_header, df_data

    def add_new_columns_to_schema(self, redshift: Redshift, tb_name: str, df_data: DataFrame, redshift_schema: dict):
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
                if d == 'int' or d == 'long':
                    df_data = df_data.withColumn(
                        column, ColumnDataTypeCheck.str_to_long(df_data[column]))
                elif d == 'double':
                    df_data = df_data.withColumn(
                        column, ColumnDataTypeCheck.str_to_double(df_data[column]))
                else:
                    df_data = df_data.withColumn(
                        column, df_data[column].cast(d))
        return df_data

    def fill_lost_columns(self, schema: dict, df_data: DataFrame):
        # 有删除列，则要在数据中添加删除的列，值设置为空
        deleted_columns = [c for c in schema if c not in df_data.columns]

        if len(deleted_columns) > 0:
            for column in deleted_columns:
                dtype = schema[column]
                d = ColumnDataTypeCheck.to_spark_dtype(dtype)
                df_data = df_data.withColumn(column, F.lit(None).cast(d))
        return df_data

    def update_column_dtype(self, redshift_schema: dict, df_data: DataFrame):
        for column in df_data.columns:
            if column not in redshift_schema:
                continue

            dtype = redshift_schema[column]
            d = ColumnDataTypeCheck.to_spark_dtype(dtype)
            if d == 'int' or d == 'long':
                df_data = df_data.withColumn(
                    column, ColumnDataTypeCheck.str_to_long(df_data[column]))
            elif d == 'double':
                df_data = df_data.withColumn(
                    column, ColumnDataTypeCheck.str_to_double(df_data[column]))
            else:
                df_data = df_data.withColumn(column, df_data[column].cast(d))
        return df_data

    def get_tb_name(self, s3_path: str):
        path_parts = s3_path.split("/")
        count = len(path_parts)

        file_name = path_parts[count-1]

        parts = self.reg_ex.split(file_name)
        used_parts = [item for item in parts if item.strip() != '']
        ll = len(used_parts)
        version = used_parts[ll-2]
        tb_name = "_".join(used_parts[:ll-2]).lower()

        file_date = path_parts[count-2]
        return tb_name, file_date, version

    def get_data(self, spark: SparkSession, s3_path: str) -> DataFrame:

        df_raw = spark.read.option(
            "multiLine", "true"
        ).option(
            "header", "true"
        ).option(
            "encoding", 'GBK'
        ).csv(s3_path)
        df_raw = df_raw.drop("English")

        return df_raw

    def do_task(self, file_s3_path: str, w_data):
        # 表明从dynamodb 去获取
        target_tb, tb_date, version = self.get_tb_name(file_s3_path)

        df_raw = self.get_data(self.spark, file_s3_path)
        df_raw.printSchema()

        print("===============> begin format")
        formater = ColumnFormater()
        df_raw = formater.remove_unused_columns(df_raw)

        if target_tb.startswith("xiaoshouyi") or target_tb.startswith("mp"):
            df_header, df_data = self.split_df_header(self.spark, df_raw)
            df_header = formater.run(df_header)
        else:
            df_header, df_data = None, df_raw

        df_data = formater.run(df_data)
        print("===============> end format")
        redshift = Redshift(region_name, self.secret_arn,
                            self.redshift_secret_name,
                            self.redshift_db,
                            self.redshift_endpoint,
                            self.redshift_temp_s3)
        schema = redshift.schema(target_tb)
        print("===============> check schema")

        # 默认情况，由于有大量缺失值，数据类型都是string
        if not schema:
            # 如果没有在 db 创建过该表，则需要根据数据采样，来修正数据的类型信息
            column_checker = ColumnDataTypeCheck()
            column_with_dtype = column_checker.run(df_data)
            df_data = self.update_column_dtype(column_with_dtype, df_data)
            print("===============> update schema from sampling")

        else:
            print("=============> schema exist in redshift")
            print(schema)
            # 如果db 中已经存在该表，则根据数据库中的表schema，更新数据的schema
            df_data = self.update_column_dtype(schema, df_data)
            # 有新增列，则修改redshift 表结构，添加新列,并根据采样修正数据中新增列的数据类型
            df_data = self.add_new_columns_to_schema(
                redshift, target_tb, df_data, schema)

            # 有删除列，则要在数据中添加删除的列，值设置为空
            df_data = self.fill_lost_columns(schema, df_data)
            print("===============> update schema from redshift")

        df_data.printSchema()

        history_tb_name = target_tb + "_history"

        # 写入数据
        w_data(redshift, target_tb, history_tb_name, df_data, tb_date, version)

        # 如果是第一次写入数据后，则需要添加注释信息
        if not schema and df_header:
            time.sleep(1)
            comments = self.get_column_chinese(df_header)
            redshift.comments(target_tb, comments)
            redshift.comments(history_tb_name, comments)
