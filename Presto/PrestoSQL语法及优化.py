"""
Created with PyCharm.
Author: CC
E-mail: 203717588@qq.com
Date: 2024/1/13
Time: 18:03
Description:
"""

# PrestoSQL使用常见问题

# 1）字段名引用
# 避免字段名与关键字冲突：MySQL对于关键字冲突的字段名加反引号，Presto对与关键字冲突的字段名加双引号。当然，如果字段名不与关键字冲突，则可以不加双引号

# 2）时间函数
# 对于timestamp，需要进行比较的时候，需要添加timestamp 关键字，而MySQL中对timestamp可以直接进行比较
'''
-- MySQL写法
SELECT t FROM a WHERE t > '2023-12-01 00:00:00'; 
-- Presto写法
SELECT t FROM a WHERE t > timestamp '2023-12-01 00:00:00';
'''
# 3）MD5函数的使用
# Presto中MD5函数传入的是binary类型，返回的也是binary类型，对字符串进行MD5操作时，需要转换
'''
SELECT to_hex(md5(to_utf8('1314')));
'''
# 4）INSERT OVERWRITE语法
# Presto中不支持insert overwrite语法，只能先delete，然后insert into

# PrestoSQL优化

# 1、存储优化

# 1）合理设置分区
# 与Hive类似，Presto会根据元数据信息读取分区数据，合理地设置分区能减少Presto数据读取量，提升查询性能
# 2）使用ORC格式存储
# Presto对ORC文件读取进行了特定优化，因此，在Hive中创建Presto使用的表时，建议采用ORC格式存储ORC和Parquet都支持列式存储，但是ORC对Presto支持更好（Parquet对Impala支持更好）
# 对于列式存储而言，存储文件是二进制的，对于经常增删字段的表，建议不要使用列式存储（修改文件元数据代价大）
# 3）使用Snappy压缩
# 数据压缩可以减少节点间数据传输对I/O带宽的压力，对于即席查询需要快速解压，建议采用Snappy压缩
# 4）预先排序
# 对于已经排序的数据，在查询的数据过滤阶段，ORC格式支持跳过读取不必要的数据。例如，对于经常需要过滤的字段可以预先排序
'''
INSERT INTO table nation_orc partition(p) SELECT * FROM nation SORT BY n_name;
'''
# 如果需要过滤n_name字段，则性能将提升：
'''
SELECT count(*) FROM nation_orc WHERE n_name='Admin';
'''

# 2、查询优化
# 1）只选择需要的字段
# 由于采用列式存储，所以只选择需要的字段可加快字段的读取速度，减少数据量。避免采用*读取所有字段
'''
[BAD]
SELECT * FROM table_1;
[GOOD]
SELECT uid,name,gender FROM table_1;
'''
# 2）使用分区字段进行过滤
# 如果数据被归档到HDFS中，并带有分区字段，在每次查询归档表的时候，带上分区字段作为过滤条件可以加快查询速度。这样可以避免Presto全区扫描，减少Presto需要扫描的HDFS的文件数
'''
[BAD]
SELECT uid,name,gender FROM table_1 where visit_date='20231201';
[GOOD]
SELECT uid,name,gender FROM table_1 where t_partition='20231201';
'''
# 3）Group By优化
# 合理安排Group by语句中字段顺序对性能有一定提升。将Group By语句中字段按照每个字段Distinct数据多少进行降序排列
'''
[BAD]
SELECT uid,gender,count(1) FROM table_1 GROUP BY gender,uid;
[GOOD]
SELECT uid,gender,count(1) FROM table_1 GROUP BY uid,gender;
'''
# 4）Order by优化
# Order by需要扫描数据到单个Worker节点进行排序，导致单个Worker需要大量内存。如果是查询Top N或Bottom N，使用limit可减少排序计算和内存压力
'''
[BAD]
SELECT * FROM table_1 ORDER BY visit_date;
[GOOD]
SELECT * FROM table_1 ORDER BY visit_date LIMIT 100;
'''
# 5）Like优化
# Presto查询优化器没有对多个like语句进行优化，使用regexp_like代替多个like语句对性能有较大提升
'''
[BAD]
SELECT * FROM access WHERE method LIKE '%GET%' OR method LIKE '%POST%';
[GOOD]
SELECT * FROM access WHERE regexp_like(method, 'GET|POST');
'''
# 6）Join优化
# Presto中Join的默认算法是broadcast join，即将Join左边的表分割到多个Worker ，然后将Join右边的表数据整个复制一份发送到每个Worker进行计算。如果右边的表数据量过大，则可能会报OOM错误，因此使用Join语句时将大表放在左边
'''
[BAD]
SELECT * FROM small_table s JOIN large_table l on l.id = s.id;
[GOOD] 
SELECT * FROM large_table l join small_table s on l.id = s.id;
'''
# 7）Row_Number优化
# 在进行一些分组排序计算排名场景时，使用rank函数性能更好
'''
[BAD]
SELECT * FROM (
  SELECT *,row_number() OVER (PARTITION BY uid ORDER BY visit_date DESC) AS rk FROM table_1
) t
WHERE rk = 1
[GOOD]
SELECT * FROM (
  SELECT *,rank() OVER (PARTITION BY uid ORDER BY visit_date DESC) AS rk FROM table_1
) t
WHERE rk = 1
'''
# 8）子查询优化
# 使用Presto分析统计数据时，可以考虑把多次查询合并为一次查询，使用Presto提供的With子查询完成
# 利用子查询，可以减少读表的次数，尤其是大数据量的表，将使用频繁的表作为一个子查询抽离出来，避免多次 Read。例如：
'''
WITH subquery_1 AS (
    SELECT a1, a2 FROM Table_1 WHERE a2 between 20231001 and 20231031
),
subquery_2 AS (
    SELECT b1, b2 FROM Table_2 WHERE b2 between 20231201 and 20231231
)     
SELECT subquery_1.a1, subquery_1.a2, subquery_2.b1, subquery_2.b2
FROM subquery_1 JOIN subquery_2 ON subquery_1.a3 = subquery_2.b3;
'''

