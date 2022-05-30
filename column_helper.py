from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, date
import re


class ColumnDataTypeCheck:
    def __init__(self):
        self.sample_ratio = 0.1
        self.max_check_count = 5
        self.bool_char = ['y', 'n', "true", "false"]
        self.datetime1_format = "%Y-%m-%d %H:%M:%S"
        self.datetime2_format = "%Y-%m-%d %H:%M"
        self.date_format = "%Y-%m-%d"
        self.string_length = 1024

    def run(self, df: DataFrame, check_columns: list = None) -> dict:
        result = dict()
        column_names = check_columns if check_columns else df.columns

        for column_name in column_names:

            dfu = df.select(column_name)

            dfu = dfu.filter(f'{column_name} is NOT NULL')
            dfsample = dfu.sample(self.sample_ratio)

            if dfsample.count() == 0:
                rows = dfu.collect()
            else:
                rows = dfsample.collect()

            count = len(rows)
            check_count = min(count, self.max_check_count)

            intCount = 0
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
                    if self.is_int(v):
                        intCount += 1
                        continue

                    if self.is_float(v):
                        floatCount += 1
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

            result[column_name] = f'VARCHAR({self.string_length})'
            for item in [(intCount, "BIGINT"), (floatCount, "REAL"), (boolCount, "BOOLEAN"), (datetimeCount, "TIMESTAMP"), (dateCount, "DATE")]:
                if item[0] == check_count:
                    result[column_name] = item[1]
                    break

        return result

    def is_float(self, item: str) -> bool:
        try:
            float(item)
            return True
        except:
            return False

    def is_int(self, item: str) -> bool:
        try:
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

        return res


class ColumnFormater:

    def run(self, df: DataFrame) -> DataFrame:
        df = self.format_column_name(df)
        df = self.update_duplicate_columns(df)
        return df

    def update_duplicate_columns(self, df: DataFrame) -> DataFrame:
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
        reg = "[^0-9A-Za-z_]"
        column_names = df.columns

        for column_name in column_names:
            new_name = column_name
            new_name = new_name.strip()
            new_name = new_name.replace(' ', '_')
            new_name = new_name.replace('-', '_')
            new_name = new_name.replace('__', '_')
            new_name = re.sub(reg, '', new_name)

            # 首字符不能数字
            first_char = new_name[0]
            if first_char.isnumeric():
                new_name = "AT_" + new_name

            if column_name != new_name:
                df = df.withColumnRenamed(column_name, new_name)
        return df
