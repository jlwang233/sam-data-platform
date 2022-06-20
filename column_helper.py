from tokenize import Special
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType, DoubleType, LongType
from datetime import datetime, date
import re

Special_Columns = {'requesting_opp_arr': 'REAL'}


class ColumnDataTypeCheck:
    def __init__(self):
        self.sample_ratio = 0.1
        self.max_check_count = 20
        self.min_check_count = 10
        self.bool_char = ['y', 'n', "true", "false"]
        self.datetime1_format = "%Y-%m-%d %H:%M:%S"
        self.datetime2_format = "%Y-%m-%d %H:%M"
        self.date_format = "%Y-%m-%d"
        self.date2_format = "%Y/%m/%d"
        self.string_length = 1024

    def run(self, df: DataFrame, check_columns: list = None) -> dict:
        result = dict()
        column_names = check_columns if check_columns else df.columns

        for column_name in column_names:
            if column_name in Special_Columns:
                result[column_name] = Special_Columns[column_name]
                break

            dfu = df.select(column_name)

            dfu = dfu.filter(f'{column_name} is NOT NULL')
            dfsample = dfu.sample(self.sample_ratio)

            if dfsample.count() == 0:
                rows = dfu.collect()
            else:
                rows = dfsample.collect()

            count = len(rows)
            if count < self.min_check_count:
                rows = dfu.collect()

            count = len(rows)
            check_count = max(count, self.min_check_count)
            check_count = min(check_count, self.max_check_count)

            longCount = 0
            floatCount = 0
            boolCount = 0
            datetimeCount = 0
            dateCount = 0
            print(f"{column_name}={check_count}")

            loop_index = 0
            for row in rows:
                if loop_index >= check_count:
                    break

                loop_index += 1

                for v in row:
                    if self.is_float(v):
                        floatCount += 1
                        continue

                    if self.is_long(v):
                        longCount += 1
                        continue

                    if self.is_bool(v):
                        boolCount += 1
                        continue

                    if self.is_datetime(v):
                        datetimeCount += 1
                        continue

                    if self.is_date(v):
                        dateCount += 1
                        continue

                if floatCount > 0 and floatCount + longCount == check_count:
                    floatCount = floatCount + longCount
                    longCount = 0

            result[column_name] = f'VARCHAR({self.string_length})'

            for item in [(longCount, "BIGINT"), (floatCount, "REAL"), (boolCount, "BOOLEAN"), (datetimeCount, "TIMESTAMP"), (dateCount, "DATE")]:
                if item[0] == check_count:
                    result[column_name] = item[1]
                    break
        print(result)
        return result

    def to_spark_dtype(dtype: str) -> str:
        check = dtype.lower()
        if check == "bigint":
            return "long"
        if check == "int":
            return "int"
        if check == "real" or check.startswith("double"):
            return "double"
        if check == "boolean":
            return "boolean"
        if check == "date":
            return "date"
        if check.startswith("timestamp"):
            return "timestamp"

        return "string"

    def is_float(self, item: str) -> bool:
        try:
            if item.find(".") > 0:
                item = item.replace(',', '')
                float(item)
                return True
            return False
        except:
            return False

    def is_long(self, item: str) -> bool:
        try:
            item = item.replace(',', '')
            int(item)
            return True
        except:
            return False

    def is_bool(self, item: str) -> bool:
        return item.lower() in self.bool_char

    def is_datetime(self, item: str) -> bool:

        try:
            res = bool(datetime.strptime(item, self.datetime1_format))
            return True
        except ValueError:
            res = False

        try:
            res = bool(datetime.strptime(item, self.datetime2_format))
            return True
        except ValueError:
            res = False

        return res

    def is_date(self, item: str) -> bool:
        try:
            res = bool(datetime.strptime(item, self.date_format))
        except ValueError:
            res = False

        if not res:
            try:
                res = bool(datetime.strptime(item, self.date2_format))
            except ValueError:
                res = False

        return res

    @staticmethod
    @F.udf(returnType=LongType())
    def str_to_long(numStr):
        if numStr:
            if len(numStr) > 3:
                numStr = numStr.replace(',', '')
            try:
                u = int(numStr)
                return u
            except Exception as ex:
                print(f"failed to convert string to int due to {ex}")
                return None
        return None

    @staticmethod
    @F.udf(returnType=DoubleType())
    def str_to_double(numStr):
        if numStr:
            if len(numStr) > 3:
                numStr = numStr.replace(',', '')

            try:
                return float(numStr)
            except Exception as ex:
                print(f"failed to convert string to int due to {ex}")
                return None
        return None


class ColumnFormater:
    def __init__(self):
        self._reg_ex = re.compile('[^A-Za-z0-9]')

    def run(self, df: DataFrame) -> DataFrame:
        df = self.format_column_name(df)
        df = self.update_duplicate_columns(df)
        return df

    def remove_unused_columns(self, df: DataFrame) -> DataFrame:
        df = df.drop("English")

        removed = [column for column in df.columns if len(column.strip()) == 0 or (
            column.startswith('_c') and column[2:].isnumeric())]
        df = df.drop(*removed)
        return df

    def update_duplicate_columns(self, df: DataFrame) -> DataFrame:
        duplicate_columns = dict()
        # 查找重复列
        column_names = df.columns
        print(column_names)
        column_l = len(column_names)
        for index in range(0, column_l):
            column_name = column_names[index]
            column_index = index + 1
            if column_name.endswith(str(column_index)):
                end_pos = len(column_name) - len(str(column_index))
                key = column_name[0: end_pos]
                if key in duplicate_columns:
                    duplicate_columns[key].append(column_name)
                else:
                    duplicate_columns[key] = [column_name]

        # 对于重复列，值不为空的最多的那一列作为主列，其余为附属列
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

    def format_column_name(self, df: DataFrame) -> DataFrame:
        column_names = df.columns

        check_names = {column_names[i].lower(
        ): i for i in range(0, len(column_names))}
        format_names = dict()

        index = 0
        for column_name in column_names:

            parts = self._reg_ex.split(column_name)
            good_parts = [item for item in parts if item.strip() != '']
            new_name = '_'.join(good_parts)

            # 首字符不能数字
            first_char = new_name[0]
            if first_char.isnumeric():
                new_name = "AT_" + new_name

            # 全小写
            new_name = new_name.lower()
            if column_name != new_name:
                # 如果有其他列的名字和修正后的列名称相同，则要修改名称
                if new_name in check_names and check_names[new_name] != index:
                    new_name = new_name + str(index + 1)
                format_names[new_name] = column_name

            index += 1

        for key in format_names:
            column_name = format_names[key]
            df = df.withColumnRenamed(column_name, key)

        return df
