# SAM  DATA PLATFORM

## 文件说明
1. event.py
作用：监听存放原始数据csv文件的s3桶,当有新的文件上传到该桶的时候，会触发lambda_handler 执行
流程：将新增文件的文件s3地址发送到sqs, 然后启动glue workflow

2. glue.py
作用：处理上传到s3里的csv文件
流程：
a. 被lambda启动后，从sqs获取新增的csv文件信息，并更新mongdb从而记录任务状态
b. 创建ETL 实例，进行具体的数据处理。最后把处理后的结果写入redshift
c. 更新mongdb从记录任务处理后的状态

3. etl.py
作用： 具体的etl处理逻辑
流程：
a. 用 spark 从 s3 读数据生成一个dataframe
b. 创建 ColumnFormater （定义在文件column_helper.py）对象,对数据信息格式化包括： 删除脏数据列，对列名命名不好的列重命名列名（例如删除列名中的空格，非法字符，数字开头的列名前面加at等等）, 对有相同列名进行重命名（重复的列，那个列的数据空值多，它的名字就加数字），还要其他操作，可以参考代码
c. 创建 Redshift 实例，获取该数据表对应的schema.
d. 如果有schema, 对比新的数据的schema 是否匹配，如果不匹配则要根据新数据的schema更新redshift schema
e. 如果没有schema,则创建 ColumnDataTypeCheck 对象，通过对数据列中的数据采样统计，获取该数据列真正的数据类型，并在redshift 根据这个schema创建表
f. 写入数据到redshift
g. 更新列的注释

4. redshift.py
一些对redshift进行操作的帮助对象，包括获取表schema, 添加注释等等

5. column_helper.py
一些针对数据的帮助函数，例如： 列名规范化，统计列数据，进行数据类型推断

6. secret.py
secretmanager 服务的帮助文件，从secretmanager 获取redshift账号密码信息。参考aws secretmanager的官方文档，了解一下 secretmanager 如何使用




