# 需要的资源和服务
1. redshift 集群
2. redshift-data  interface endpoint
3. sqs
5. dynomaDB
6. glue
7. s3
8. secretmanager

# 环境搭建
1. 创建redshift 集群
2. 使用secretmanager 保存redshift 账号密码
3. 在vpc 控制台上创建 redshift 
3. 创建sqs
4. 在dynamodb 中创建 名称为 sam-data-upload-event  的表
表结构为：
```
key: file_key string
sort key: event_time string
```
5. 创建名为 sam-data-files 的s3 桶 用于ops团队上传数据文件
6. 创建名为 sam-data-redshift-temp 的s3 桶 用于redshift保存临时数据

# 修改代码文件
1. 修改glue.py 文件
修改代码中 标注的参数
```
# ===========================需要修改的配置参数================================
```

a. 在 secretmanager 复制刚才保存了redshift账号和密码的 secret 名称，赋值给 redshift_secret_name, 并把该 secret 的arn 赋值给 secret_arn

b. redshift_db 创建的redshift集群的db 名称，默认应该是 dev

c. 

