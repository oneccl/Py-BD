"""
Created with PyCharm.
Author: CC
E-mail: 203717588@qq.com
Date: 2023/10/8
Time: 22:27
Description:
"""

# Python连接Hive

# 使用Impala查询引擎

# Impala通过Thrift和Hive Metastore服务端建立连接，获取元数据信息，进而与HDFS交互
# Thrift是一个轻量级、跨语言的RPC框架，主要用于服务间的RPC通信。由Facebook于2007年开发，2008年进入Apache开源项目

# sasl模块是Python中用于实现SASL（Simple Authentication and Security Layer）认证的第三方库，提供了对各种SASL机制的支持，例如与Kafka、Hadoop等进行安全通信
'''
pip install bitarray
pip install bit_array
pip install thrift
pip install thriftpy
pip install pure_sasl
pip install --no-deps thrift-sasl==0.2.1
'''

# 安装Impyla库：pip install impyla
'''
Python-whl大全：https://www.lfd.uci.edu/~gohlke/pythonlibs/
'''

from impala.dbapi import connect
from impala.util import as_pandas

# 连接Hive
conn = connect(host='bd91', port=10000, auth_mechanism='PLAIN', user="root", password="123456", database="default")
# 创建游标
cursor = conn.cursor()
# 执行查询
cursor.execute("select * from stu")
# 将结果转换为Pandas DataFrame
df = as_pandas(cursor)
print(df.to_string())
# 执行查询
cursor.execute("select * from hivecrud.s_log limit 3")
# 结果转换为Pandas DataFrame
df = as_pandas(cursor)
print(df.to_string())
# 关闭连接
cursor.close()
conn.close()

