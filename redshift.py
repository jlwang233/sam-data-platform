from secret import get_secret
import boto3
import time


class Redshift:
    def __init__(self, region_name: str, secret_arn: str, redshift_secret_name: str, db_name: str, end_point: str, s3_temp: str):
        redshift_info = get_secret(redshift_secret_name, region_name)

        print("===============> got redshift_info")
        self.redshift_identifier = redshift_info['dbClusterIdentifier']
        self.redshift_jdbc = f"jdbc:redshift://{redshift_info['host']}:{redshift_info['port']}/dev"
        self.redshift_user = redshift_info['username']
        self.redshift_pass = redshift_info['password']
        self.redshift_db = db_name
        self.secret_arn = secret_arn
        self.client = boto3.client(
            'redshift-data',
            region_name=region_name,
            endpoint_url=end_point,
        )
        self.s3_temp = s3_temp

    def query(self, sql: str, timeout=15) -> list:
        response = self.client.execute_statement(
            ClusterIdentifier=self.redshift_identifier,
            Database=self.redshift_db,
            SecretArn=self.secret_arn,
            Sql=sql,
            WithEvent=False
        )
        if "Id" not in response:
            return

        result_id = response["Id"]

        response2 = self.client.describe_statement(
            Id=result_id
        )
        if "Status" not in response2:
            return

        index = 0
        while response2['Status'] != 'FINISHED' and index < timeout * 2:
            time.sleep(0.5)
            response2 = self.client.describe_statement(
                Id=result_id
            )
            index += 1

        response3 = self.client.get_statement_result(
            Id=result_id
        )
        print(response3)
        if 'Records' not in response3:
            return []

        return [[list(item.values())[0] for item in row] for row in response3['Records']]

    def exec(self, sql: str) -> bool:

        response = self.client.execute_statement(
            ClusterIdentifier=self.redshift_identifier,
            Database=self.redshift_db,
            SecretArn=self.secret_arn,
            Sql=sql,
            WithEvent=False
        )
        print(response)
        if 'ResponseMetadata' not in response:
            return False

        if 'HTTPStatusCode' not in response['ResponseMetadata']:
            return False

        if response['ResponseMetadata']['HTTPStatusCode'] not in [200, 201]:
            return False

        return True

    def comments(self, tb_name: str, comments: dict) -> bool:
        sqls = [
            f"comment on COLUMN {tb_name}.{column} is '{comment}';"
            for column, comment in comments.items()
        ]

        self.exec_bath(sqls)

    def add_columns(self, tb_name: str, columns: dict) -> bool:
        sqls = [
            f"alter table {tb_name} add column {column} {dtype};"
            for column, dtype in columns.items()
        ]
        self.exec_bath(sqls)

    def exec_bath(self, sqls: list) -> bool:
        bach_exec_count = 40  # ?????????????????????40???sql

        index = 0
        total_index = 0
        commands = list()
        for sql in sqls:
            commands.append(sql)
            index += 1
            total_index += 1
            if index >= bach_exec_count or total_index == len(sqls):
                response = self.client.batch_execute_statement(
                    ClusterIdentifier=self.redshift_identifier,
                    Database=self.redshift_db,
                    SecretArn=self.secret_arn,
                    Sqls=commands,
                    WithEvent=False
                )
                print(response)
                if 'ResponseMetadata' not in response:
                    return False

                if 'HTTPStatusCode' not in response['ResponseMetadata']:
                    return False

                if response['ResponseMetadata']['HTTPStatusCode'] not in [200, 201]:
                    return False

                index = 0
                commands = list()

        return True

    def schema(self, tb_name: str):
        sql = f"""SELECT *
               FROM pg_table_def
               WHERE schemaname='public'
               and tablename='{tb_name}'"""
        rows = self.query(sql)
        if len(rows) == 0:
            return None

        schema = dict()
        for row in rows:
            schema[row[2]] = row[3]
        return schema

    def create(self, tb_name: str, columns: list, schema: dict) -> str:
        sql_list = list()
        sql_list.append(f"CREATE TABLE IF NOT EXISTS {tb_name} (")

        # columns ?????????????????????
        index = 1
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

    def tables(self):
        response = self.client.list_tables(
            ClusterIdentifier=self.redshift_identifier,
            ConnectedDatabase=self.redshift_db,
            Database=self.redshift_db,
            MaxResults=1000,
            SecretArn=self.secret_arn

        )
        return response

    def conn_option(self, tb_name: str, write_mode='append'):
        conn = {
            "url": self.redshift_jdbc,
            "dbtable": tb_name,
            "user": self.redshift_user,
            "password": self.redshift_pass,
            "redshiftTmpDir": f"{self.s3_temp}/temp/{tb_name}/"
        }
        if write_mode == "overwrite":
            conn["preactions"] = f"TRUNCATE TABLE {tb_name}"
        return conn
