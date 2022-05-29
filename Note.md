```
1. 如何推断数据类型 ？
 问题产生原因：默认数据类型都是string,由于有大量空值，所以即使数字类型，也推断成了string
2. 新增字段如何推断数据类型
 问题产生原因：同问题1
 
3.如何更新schema

4.需要处理数据列名的非法字符，空格等等

5.提取中文




```
开发过程中遇到的问题
```
Pyspark: ValueError: Some Of Types Cannot Be Determined After Inferring

原因，列全为空
```