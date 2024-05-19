"""
Created with PyCharm.
Author: CC
E-mail: 203717588@qq.com
Date: 2023/12/7
Time: 21:44
Description:
"""
# 多维分析：Hive增强的聚合、多维数据集、分组和汇总

# 增强聚合：在SQL中使用分组聚合查询时，使用GROUPING SETS、CUBE、ROLLUP等子句进行操作
# 多维分析：多种维度组合的分析，而不是多种维度的分析
# 多维分析主要用于多维度聚合，即多种维度组合并聚合结果

# 1、GROUPING SETS

# Hive官方对GROUPING SETS的描述：
# GROUP BY中的GROUPING SETS子句允许我们在同一记录集中指定多个GROUP BY选项。所有GROUPING SET子句都可以用由UNION连接的多个GROUP BY查询来逻辑表示
# Hive官方文档：https://cwiki.apache.org/confluence/display/Hive/Enhanced+Aggregation%2C+Cube%2C+Grouping+and+Rollup
# 简单来说就是指定多组维度作为GROUP BY的分组规则，然后再将结果联合在一起。它的效果等同于先分别对这些组维度进行GROUP BY分组后，再通过UNION将结果联合起来
# 例如，GROUPING SET查询和等效的GROUP BY查询如下：
'''
SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b GROUPING SETS ( (a,b), a)
-- 等效于
SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b
UNION
SELECT a, null, SUM( c ) FROM tab1 GROUP BY a

SELECT a,b, SUM( c ) FROM tab1 GROUP BY a, b GROUPING SETS (a,b)
-- 等效于
SELECT a, null, SUM( c ) FROM tab1 GROUP BY a
UNION
SELECT null, b, SUM( c ) FROM tab1 GROUP BY b
'''

# GROUPING SETS和UNION两个版本的SQL语句查询性能相比GROUPING SETS性能更高效。GROUPING SETS(增强GROUP BY)避免了多次读取底层表，可以降低生成Job的个数，从而减轻磁盘和网络I/O时的压力

# GROUPING SETS语法：
'''
-- Hive语法：
GROUP BY 维度组合所需列并集 GROUPING SETS (维度组合1,维度组合2,...)
-- SparkSQL、FlinkSQL、Presto语法：
GROUP BY GROUPING SETS (维度组合1,维度组合2,...)
'''
# 主要区别：
# Hive中GROUP BY后面必须添加参与分组的字段，即维度组合所需列并集
# SparkSQL、FlinkSQL、Presto中GROUP BY后面不必要跟参与分组的字段

# SparkSQL官方文档：https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html

# 2、GROUPING__ID

# Hive的GROUPING__ID函数主要用于映射到维度列名称，此函数返回一个位向量，该位向量对应于每列是否存在
# 对于GROUPING__ID递增数列中的每个数值，将该数值的2进制每个Byte位与维度名称序列的下标进行映射，输出所有对应比特位0的维度名称
# 例如：
# GROUPING__ID：3 => {0, 1, 1}
# 维度名称序列：{省份, 城市, 设备类型}
# 映射：{0:省份, 1:城市, 1:设备类型}
# GROUPING__ID为3的数据行聚合维度即为：省份

# 在Hive2.3.0前后，Hive的GROUPING__ID函数表示含义不同
# Hive2.3.0之前：
# 对于每一列，如果该列已聚合在该行中，则该函数将返回值0，否则该值为1
# 对于给定的分组，如果分组中包含相应的列，则将位设置为0，否则将其设置为1
# Hive2.3.0及之后：
# 对于每一列，如果该列已聚合在该行中，则该函数将返回值1，否则该值为0
# 对于给定的分组，如果分组中包含相应的列，则将位设置为1，否则将其设置为0

# 例如（Hive2.3.0及之后）：
'''
select province, city, device_type, count(1) counts, grouping__id 
from visit_log
group by province, city, device_type
grouping sets (
    (province),
    (province, city),
    (province, city, device_type)
)
'''
# 步骤1：按照group by后的分组顺序
# 步骤2：以grouping sets中的(province)为例，得到的二进制映射为：100
# 步骤3：100转化为10进制：4
# 步骤4：判断得到province维度值grouping__id=4

# GROUPING__ID为每个GROUPING SETS维度组合进行编号

# GROUPING__ID函数语法：
'''
-- Hive语法：
GROUPING__ID
-- SparkSQL语法：内部生成spark_grouping_id列后被删除，可使用GROUPING_ID()查询
GROUPING_ID()
-- Presto语法：
GROUPING(维度组合所需列并集)
'''
# 若GROUPING SETS中有N种组合，则GROUPING__ID、GROUPING_ID()、GROUPING()会生成N个不等的数值

# 1）Hive示例：
'''
select province, city, device_type, count(1) counts, grouping__id 
from visit_log
group by province, city, device_type
grouping sets (
    (province),
    (province, city),
    (province, city, device_type)
)
'''
# 2）SparkSQL示例：
'''
select province, city, device_type, count(1) counts, grouping_id() as grouping__id
from visit_log
group by grouping sets (
    (province),
    (province, city),
    (province, city, device_type)
)
'''
# 3）PrestoSQL示例：
'''
select province, city, device_type, count(1) counts, grouping(province, city, device_type) as grouping__id 
from visit_log
group by grouping sets (
    (province),
    (province, city),
    (province, city, device_type)
)
-- 或
select province, city, device_type, count(1) counts, grouping(province, city, device_type) as grouping__id    -- grouping()将grouping sets所有分组组合用到的字段取并集列出
from visit_log
group by province, city, device_type,    -- 此处添加逗号,
grouping sets (
    (province),
    (province, city),
    (province, city, device_type)
)
'''

# 3、ROLLUP与CUBE

# WITH ROLLUP
# ROLLUP子句与GROUP BY一起使用，主要用来计算维度层次结构级别的聚合
'''
GROUP BY a, b, c WITH ROLLUP
-- 等价于
GROUP BY a, b, c GROUPING SETS ((a, b, c), (a, b), (a), ())
'''
# ROLLUP语法：
'''
-- Hive语法：
GROUP BY 维度组合所需列并集 WITH ROLLUP
-- SparkSQL、FlinkSQL、Presto语法：
GROUP BY ROLLUP(维度组合所需列并集)
'''
# 1）Hive示例：
'''
select province, city, device_type, count(1) counts, grouping__id 
from visit_log
group by province, city, device_type
with rollup
'''
# 2）SparkSQL示例：
'''
select province, city, device_type, count(1) counts, grouping_id() as grouping__id 
from visit_log
group by 
rollup(province, city, device_type)
'''
# 3）PrestoSQL示例：
'''
select province, city, device_type, count(1) counts, grouping(province, city, device_type) as grouping__id
from visit_log
group by 
rollup(province, city, device_type)
'''

# WITH CUBE
# CUBE子句与GROUP BY一起使用，主要用于在其参数中创建列集的所有可能组合
'''
GROUP BY a，b，c WITH CUBE
-- 等价于
GROUP BY a, b, c GROUPING SETS ((a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ())
'''
# CUBE语法：
'''
-- Hive语法：
GROUP BY 维度组合所需列并集 WITH CUBE
-- SparkSQL、FlinkSQL、Presto语法：
GROUP BY CUBE(维度组合所需列并集)
'''
# 1）Hive示例：
'''
select province, city, device_type, count(1) counts, grouping__id 
from visit_log
group by province, city, device_type
with cube
'''
# 2）SparkSQL示例：
'''
select province, city, device_type, count(1) counts, grouping_id() as grouping__id 
from visit_log
group by 
cube(province, city, device_type)
'''
# 3）PrestoSQL示例：
'''
select province, city, device_type, count(1) counts, grouping(province, city, device_type) as grouping__id
from visit_log
group by 
cube(province, city, device_type)
'''
# ROLLUP和CUBE是GROUPING SETS的语法糖，在性能上，ROLLUP和CUBE与直接使用GROUPING SETS是一样的，但在SQL表达上更加简洁

# 4、多维分析常见问题与解决

# 1）多维聚合优化
# 由于在使用GROUPING SETS(增强GROUP BY)时，会在同一个Job中完成多种维度组合的聚合(2的N次方)，当底层表数据量太大或维度过多时，可能造成计算资源不够而导致任务失败
# 可以使用如下参数对Job进行拆分：set hive.new.job.grouping.set.cardinality=30;
# 该参数表示，指定维度组合数，当超过该阈值时， Map端会对Job进行拆分，即每个Job最多能处理的维度组合数，默认值为30
# 另外，在执行Hive的增强的聚合时，必须开启Map端合并：set hive.map.aggr=true;

# 2）有JOIN连接的多维分析

# 有JOIN连接时，需要对表JOIN连结的结果进行GROUPING SETS(增强GROUP BY)，避免与JOIN混合使用
# 例如下面SQL：
'''
select t1.province, t1.city, t2.catgory_id, t2.catgory_name sum(t1.sale), GROUPING__ID
from sales t1
left join goods t2
on t1.goods_id = t2.id
group by t1.province, t1.city, t2.catgory_id, t2.catgory_name
grouping sets (
    (t1.province),
    (t1.province, t1.city),
    (t1.province, t1.city, t2.catgory_id)
)
'''
# 正确的写法：
'''
select province, city, catgory_id, catgory_name sum(sale), GROUPING__ID from (
    select t1.province, t1.city, t2.catgory_id, t2.catgory_name, t1.sale
    from sales t1
    left join goods t2
    on t1.goods_id = t2.id
) t
-- group by字段多于grouping sets维度组合所需列并集：Hive不报错，Presto报错
group by province, city, catgory_id, catgory_name
grouping sets (
    (province),
    (province, city),
    (province, city, catgory_id)
)
'''
# 假设表t结果有5条记录，按照上面语句（3种组合）执行，结果记录数应为5×3=15条记录

# 3）其他使用注意

# 和GROUP BY一样，使用GROUPING SETS(增强GROUP BY)的SQL，没有在分组中字段不支持在SELECT子句中列出
# Hive、SparkSQL、PrestoSQL的GROUPING SETS(增强GROUP BY)语法较相似，使用时避免混淆而混合使用


# 参考文章：
# https://blog.csdn.net/LdyLly/article/details/124351342
# https://blog.csdn.net/weixin_42845827/article/details/132970245
# https://zhuanlan.zhihu.com/p/536981356



