"""
Created with PyCharm.
Author: CC
E-mail: 203717588@qq.com
Date: 2023/11/19
Time: 14:48
Description:
"""
# PyHive模块
# PyHive通过与HiveServer2通讯来操作Hive数据。当hiveserver2服务启动后，会开启10000的端口，对外提供服务，此时PyHive客户端通过JDBC连接hiveserver2进行Hive SQL操作

import pandas as pd
from pyhive import hive

# 连接Hive服务端
conn = hive.Connection(host='bd91', port=10000, database='default')
cursor = conn.cursor()

# 执行HQL
cursor.execute("select * from stu")

# 转换为Pandas DataFrame
columns = [col[0] for col in cursor.description]
result = cursor.fetchall()

df = pd.DataFrame(result, columns=columns)
print(df.to_string())


# https://blog.csdn.net/qq_29425617/article/details/114451558
