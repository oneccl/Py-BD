
# PySpark
# 官网：https://spark.apache.org/docs/3.1.2/api/python/getting_started

# 开发环境搭建

# 前提：确保已经安装配置了Java和Scala

# 1）Hadoop的Windows环境配置
"""
由于Hadoop主要基于Linux编写，而Hive、Spark等依赖于Hadoop，因此，Hadoop在Windows上运行需要winutils.exe和hadoop.dll等环境文件的支持
winutils.exe和hadoop.dll等文件必须放置在bin目录下，主要用于模拟Linux下的目录环境
官方文档说明：https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems
"""
# 配置Hadoop的Windows环境变量：
# JAVA_HOME=D:\Program Files\Java\jdk1.8.0_311
# HADOOP_HOME=D:\Software\Hadoop\hadoop-2.7.7
# 将hadoop.dll和winutils.exe文件拷贝到C:\Windows\System32目录中，重启电脑

# 各版本hadoop.dll和winutils.exe下载：https://github.com/cdarlint/winutils/tree/master/hadoop-3.1.2

# 安装：pip install pyspark==3.1.2

# 报错：org.apache.spark.SparkException: Python worker failed to connect back.
# 解决：确保已经配置Windows系统环境变量（配置后需要重启PyCharm）：
# 配置PySpark的Windows环境变量：
# PYSPARK_PYTHON=python
# PYSPARK_DRIVER_PYTHON=jupyter
# PYSPARK_DRIVER_PYTHON_OPTS=lab

# 验证：Win+R：cmd回车输入命令：spark-shell

import re

import numpy as np
from pyspark import SparkContext, SparkConf

# 1、PySpark批处理（本地单机模式）
# SparkConf入口

# 单词统计
conf = SparkConf().setMaster("local[*]").setAppName("WordCount")
sc = SparkContext(conf=conf)
rdd_lines = sc.textFile(r'C:\Users\cc\Desktop\temp\HarryPotter.txt')
rdd_lines.flatMap(lambda line: re.split("\\s+", re.sub("\\W+", " ", line.lower())))\
    .filter(lambda w: w.strip() != "")\
    .map(lambda w: (w, 1))\
    .reduceByKey(lambda v1, v2: v1+v2)\
    .sortBy(lambda t: t[1], ascending=False, numPartitions=1)\
    .foreach(lambda t: print(t))

# 2、SparkSQL
# SparkSession入口

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder\
    .master("local[*]")\
    .appName("SparkSQL")\
    .enableHiveSupport()\
    .getOrCreate()

# 1）创建PySpark DataFrame
from datetime import date

# a、创建数据帧：从Row对象列表创建
df_row = spark.createDataFrame([
    Row(id=100, name='Tom', birth=date(1988, 10, 20)),
    Row(id=101, name='Jerry', birth=date(1990, 12, 1)),
    Row(id=102, name='Bob', birth=date(1978, 5, 3))
])
print(df_row)     # DataFrame[id: bigint, name: string, birth: date]
df_row.show()

# b、创建数据帧：使用显示架构创建
df = spark.createDataFrame([
    (100, "Tom", date(1988, 10, 20)),
    (101, "Jerry", date(1990, 12, 1)),
    (102, "Bob", date(1978, 5, 3))
], schema="id bigint, name string, birth date")
print(df)     # DataFrame[id: bigint, name: string, birth: date]
df.show()

# c、创建数据帧：从Pandas数据帧创建
# 将Pandas的DataFrame转换为Spark的DataFrame

import pandas as pd

df_pandas = pd.DataFrame({
    'id': [100, 101, 102],
    'name': ["Tom", "Jerry", "Bob"],
    'birth': [date(1988, 10, 20), date(1990, 12, 1), date(1978, 5, 3)]
})
df_spark = spark.createDataFrame(df_pandas)
print(df_spark)     # DataFrame[id: bigint, name: string, birth: date]
df_spark.show()

# 报错：AttributeError: 'DataFrame' object has no attribute 'iteritems'.
# 原因：最新版Pandas弃用了iteritems()
# 解决：重新安装1.5.3版本

# 2）数据查看与转换
# 查看数据
df.show(1)
'''
+---+----+----------+
| id|name|     birth|
+---+----+----------+
|100| Tom|1988-10-20|
+---+----+----------+
only showing top 1 row
'''
# 查看列名
print(df.columns)
'''
['id', 'name', 'birth']
'''
# 查看列详情
df.printSchema()
'''
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- birth: date (nullable = true)
'''
# 取开头结尾行
print(df.take(1))
'''
[Row(id=100, name='Tom', birth=datetime.date(1988, 10, 20))]
'''
print(df.tail(1))
'''
[Row(id=102, name='Bob', birth=datetime.date(1978, 5, 3))]
'''
# 转换为Pandas数据帧
# 会将所有数据收集到Driver端，当数据太大而无法放入Driver端时，容易导致OOM错误
print(df.toPandas())
'''
    id   name       birth
0  100    Tom  1988-10-20
1  101  Jerry  1990-12-01
2  102    Bob  1978-05-03
'''

# 3）数据操作

# 数据选择、访问
df.select(df.id).show()
'''
+---+
| id|
+---+
|100|
|101|
|102|
+---+
'''
df.filter(df.birth > "1990-01-01").show()
'''
+---+-----+----------+
| id| name|     birth|
+---+-----+----------+
|101|Jerry|1990-12-01|
+---+-----+----------+
'''
# 分配新的一列
df.withColumn("new_col", df.id+1).show()
'''
+---+-----+----------+-------+
| id| name|     birth|new_col|
+---+-----+----------+-------+
|100|  Tom|1988-10-20|    101|
|101|Jerry|1990-12-01|    102|
|102|  Bob|1978-05-03|    103|
+---+-----+----------+-------+
'''
# Spark DataFrame分组
df.groupby(['name']).sum('id').show()
'''
+-----+-------+
| name|sum(id)|
+-----+-------+
|  Tom|    100|
|Jerry|    101|
|  Bob|    102|
+-----+-------+
'''


# 3、文件读写操作

# 1）文本文件读写
'''
df_text = spark.read.text('path')
df_text.show()
df_text.write.text('path')
'''

# 示例1：读取本地文本文件
# 读取文本，返回Spark DataFrame
df_text = spark.read.text(r'C:\Users\cc\Desktop\pyspark.txt')
df_text.show()
# Spark DataFrame写入文本文件
df_text.write.text(r'C:\Users\cc\Desktop\write_text')

# 示例2：读取HDFS文件：hdfs://bd91:8020/Harry.txt
df_text = spark.read.text('hdfs://bd91:8020/Harry.txt')
df_text.show()

# 示例3：读取Hive表：hdfs://bd91:8020/user/hive/warehouse/usersinfo/000000_0
df_text = spark.read.text('hdfs://bd91:8020/user/hive/warehouse/usersinfo/000000_0')
df_text.show()

# 2）CSV文件读写
'''
df_csv = spark.read.csv('path', header=True)
df_csv.show()
df_csv.write.csv('path', header=True)
'''

# 3）Parquet文件读写
'''
df_parquet = spark.read.parquet('path')
df_parquet.show()
df_parquet.write.parquet('path')
'''

# 4）ORC文件读写
'''
df_orc = spark.read.orc('path')
df_orc.show()
df_orc.write.orc('path')
'''

# PySpark支持的其它数据源：
# SparkSQL Data Sources：https://spark.apache.org/docs/3.1.2/sql-data-sources.html

# 4、PySparkSQL

# 1）SQL操作
# SparkSQL所需依赖：pip install pyarrow

df_text.createTempView('t1')
sql = """select * from t1"""
spark.sql(sql).show()

# UDF可以在SQL中注册和调用，开箱即用
from pyspark.sql.pandas.functions import pandas_udf
import pandas as pd

df.createTempView("t2")
# 自定义UDF函数应用于SQL
@pandas_udf("bigint")
def add_one(s: pd.Series) -> pd.Series:
    return s + 1

# 注册
spark.udf.register("add_one", add_one)

# 使用
spark.sql("select add_one(id) from t2").show()
'''
+-----------+
|add_one(id)|
+-----------+
|        101|
|        102|
|        103|
+-----------+
'''

# 2）DSL：逻辑SQL
from pyspark.sql.functions import expr

df.selectExpr('add_one(id)').show()
'''
+-----------+
|add_one(id)|
+-----------+
|        101|
|        102|
|        103|
+-----------+
'''
df.select(expr('count(*)') > 0).show()
'''
+--------------+
|(count(1) > 0)|
+--------------+
|          true|
+--------------+
'''
# DSL也可应用UDF函数
# PySpark支持各种Pandas UDF和API
@pandas_udf('bigint')
def pandas_add_one(s: pd.Series) -> pd.Series:
    return s + 1

df.select(pandas_add_one(df.id)).show()
'''
+------------------+
|pandas_add_one(id)|
+------------------+
|               101|
|               102|
|               103|
+------------------+
'''


# 5、PySpark On Hive
# PySpark远程连接Hive数据仓库

# 解决方案
"""
Spark提供执行引擎能力
Hive的MetaStore提供元数据管理功能
让Spark和MetaStore连接起来
"""
# 开启Hive的MetaStore服务：hive --service metastore

# spark = SparkSession.builder.enableHiveSupport()\
#     .config('spark.driver.memory', '2g')\
#     .config('spark.executor.cores', 8)\
#     .config('spark.executor.memory', '4g')\
#     .config("spark.sql.session.timeZone", "UTC")\
#     .config("spark.hadoop.hive.metastore.uris", "url")\
#     .config("spark.sql.database.default", "db_name")\
#     .getOrCreate()

spark = SparkSession.builder.enableHiveSupport()\
    .config('spark.driver.memory', '2g')\
    .config('spark.executor.cores', 8)\
    .config('spark.executor.memory', '4g')\
    .config("spark.sql.session.timeZone", "UTC")\
    .config("hive.metastore.uris", "thrift://bd91:9083")\
    .config("spark.sql.database.default", "default")\
    .getOrCreate()

spark.sql("""select * from stu""").show()
'''
+----+---+
|name|age|
+----+---+
| Tom| 18|
+----+---+
'''
spark.sql("""select * from hivecrud.s_log""").show(3)
'''
+---------------+-----+-------+-------+
|             ip|count|code200| upload|
+---------------+-----+-------+-------+
|  111.18.41.158|  138|    131|7715925|
|167.114.173.203| 1449|   1074|6641732|
|183.195.101.211|   74|     68|4800006|
+---------------+-----+-------+-------+
'''

# 6、Pandas On Spark
# Pandas与PySpark间的转换

# Apache Arrow是一种内存中列式数据格式，用于Spark中以在JVM和Python进程之间高效传输数据。目前这对于使用Pandas/NumPy数据的Python用户最有利。它的使用不是自动的，可能需要对配置或代码进行一些小的更改才能充分利用并确保兼容性
# 若要在PySpark中使用Apache Arrow，首先应安装合适的PyArrow版本。如果使用pip安装PySpark，则可以使用命令pip install pyspark[sql]将PyArrow作为SQL模块的额外依赖项引入。否则，您必须确保PyArrow已安装并在所有集群节点上可用

# 安装PyArrow：
# PyArrow目前与Python3.8、3.9、3.10和3.11兼容，从PyPI安装最新版本PyArrow（Windows、Linux、和macOS）：
# pip install pyarrow

# 当使用调用DataFrame.toPandas()将Spark DataFrame转换为Pandas DataFrame以及使用SparkSession.createDataFrame()从Pandas DataFrame创建Spark DataFrame时，Arrow可用作优化。要在执行这些调用时使用Arrow，用户需要首先将Spark配置设置为spark.sql.execution.arrow.pyspark.enabled=true。默认情况下禁用此功能

# 启用基于Arrow的列式数据传输
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# 生成Pandas数据框（3行4列二维随机数：(0,1)之间）
pdf = pd.DataFrame(np.random.rand(3, 4))
# 使用Arrow从Pandas DataFrame创建Spark DataFrame
df = spark.createDataFrame(pdf)

# 使用Arrow将Spark DataFrame转换为Pandas DataFrame
result_pdf = df.select("*").toPandas()
print(result_pdf.to_string())

# 对Arrow使用上述优化将产生与未启用Arrow时相同的结果
# 请注意，即使使用Arrow，DataFrame.toPandas()也会导致将DataFrame中的所有记录收集到驱动程序，并且应该在一小部分数据上完成。目前，并非所有Spark数据类型都受支持，如果列具有不受支持的类型，则可能会引发错误。如果SparkSession.createDataFrame()期间发生错误，Spark将回退以创建不带Arrow的DataFrame

# 其他使用参考官方文档：
# 官方文档1：https://spark.apache.org/docs/3.1.2/sql-pyspark-pandas-with-arrow.html
# 官方文档2：https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/io.html





