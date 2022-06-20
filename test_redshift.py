from redshift import Redshift


region_name = "cn-northwest-1"

# ===========================需要修改的配置参数================================
redshift_secret_name = 'dev/demo/redshift'
secret_arn = 'arn:aws-cn:secretsmanager:cn-northwest-1:027040934161:secret:dev/demo/redshift-dpZ9yS'
redshift_db = 'dev'
redshift_temp_s3 = 's3://sam-data-platform-redshift-temp'
redshift_endpoint = "https://vpce-0c20d267bb10f74e6-zlv5xrlp.redshift-data.cn-northwest-1.vpce.amazonaws.com.cn"
dynamodb_sam_data_trace_tb = 'sam-data-upload-event'
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/sam-data-plaftform-test"
sqs_endpoint_url = "https://vpce-05ddb236d804f224d-s18hyk87.sqs.cn-northwest-1.vpce.amazonaws.com.cn"
# =============================上面参数需要修改==========================================


redshift = Redshift(region_name, secret_arn,
                    redshift_secret_name, redshift_db, redshift_endpoint, redshift_temp_s3)

r = redshift.tables()
tables = r['Tables']

sqls = list()
for table in tables:
    schema = table['schema']

    if schema != 'public':
        continue
    tb_name = table['name']
    if not tb_name.startswith("xiaoshouyi"):
        continue
    redshift.exec(f'drop table public."{tb_name}";')
    print(f'drop table public."{tb_name}";')
