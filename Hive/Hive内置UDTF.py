"""
Created with PyCharm.
Author: CC
E-mail: 203717588@qq.com
Date: 2023/11/27
Time: 21:21
Description:
"""
# Hive内置表生成函数

# 在Hive中，所有的运算符和用户定义函数，包括用户定义的和内置的，统称为UDF（User-Defined Functions）
# UDF官方文档：https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF
# 其中，用户自定义聚合函数和内置聚合函数统称为UDAF（User-Defined Aggregate Functions），用户自定义表生成函数和内置表生成函数统称为UDTF（User-Defined Table-Generating Functions）

# 1）explode(array/map)
# 功能：列转行
# select explode(array(1,2,3))
# select explode(split('1,2,3', ','))
'''
col
1
2
3
'''
# select explode(map(1,2,3,4))
'''
key	value
1	2
3	4
'''
# 2）posexplode(array)
# 功能：列转行，第一列添加元素索引（从0开始）
# select posexplode(array(1,2,3))
'''
pos	val
0	1
1	2
2	3
'''
# 3）stack(n,v1,v2,…,vk)
# 功能：将k个数据平均转换成n行，即k/n列，k必须是n的整数倍，空值使用NULL
# -- 将9个元素按顺序分成3行3列
# with user_log as (
#     select stack (
#         3,
#         '1001', '2023-11-11', 123,
#         '1002', '2023-11-12', 145,
#         '1001', '2023-11-12', 143
#     )
#     as (id, dt, lowcarbon)
# )
# select * from user_log
'''
user_log.id	user_log.dt	user_log.lowcarbon
1001	     2023-11-11	               123
1002	     2023-11-12	               145
1001	     2023-11-12	               143
'''
# 4）lateral view UDTF
# 功能：UDTF只允许在SELECT后面跟UDTF，不允许在SELECT后跟其他字段，例如：
# select 'CN' as country,explode(array(1,2,3))
# Hive报错，SparkSQL不报错。lateral view可以解决这个问题
# 示例1：字符串分割
'''
# -- 方式1
with shop as (
    select '1001' as pid,'1,2,3' as svs
    union
    select '1002' as pid,'4,5,' as svs
)
select pid,svs,sv from shop
lateral view outer explode(split(svs, ',')) tmp_v as sv

# -- 方式2
select pid,svs,sv from (
    select * from (
        select '1001' as pid,'1,2,3' as svs
        union
        select '1002' as pid,'4,5,' as svs
    ) tmp
) shop
lateral view outer explode(split(svs, ',')) tmp_v as sv
'''
'''
pid  	svs	   sv
1001	1,2,3	1
1001	1,2,3	2
1001	1,2,3	3
1002	4,5,	4
1002	4,5,	5
1002	4,5,	
'''
# 方式1和方式2使用lateral view和lateral view outer效果相同，空缺值显示为空字符串''

# 示例2：数组
'''
# -- 方式1
with shop as (
    select '1001' as pid,array(1,2,3) as svs
    union
    select '1002' as pid,array(4,5,NULL) as svs
)
select pid,svs,sv from shop
lateral view outer explode(svs) tmp_v as sv

# -- 方式2
select pid,svs,sv from (
    select * from (
        select '1001' as pid,array(1,2,3) as svs
        union
        select '1002' as pid,array(4,5,NULL) as svs
    ) tmp
) shop
lateral view outer explode(svs) tmp_v as sv
'''
'''
pid	    svs	       sv
1001	[1,2,3]	    1
1001	[1,2,3]	    2
1001	[1,2,3]	    3
1002	[4,5,null]	4
1002	[4,5,null]	5
1002	[4,5,null]	NULL
'''
# 方式1和方式2使用lateral view和lateral view outer效果相同，空缺值显示为NULL

# 示例3：数据存在NULL
'''
# -- 方式1
with shop as (
    select '1001' as pid, '1,2,3' as svs
    union
    select '1002' as pid, NULL as svs
)
select pid,svs,sv from shop
lateral view outer explode(split(svs, ',')) tmp_v as sv

# -- 方式2
select pid,svs,sv from (
    select * from (
        select '1001' as pid, '1,2,3' as svs
        union
        select '1002' as pid, NULL as svs
    ) tmp
) shop
lateral view outer explode(split(svs, ',')) tmp_v as sv
'''
# -- lateral view结果：
'''
pid	    svs	   sv
1001	1,2,3	1
1001	1,2,3	2
1001	1,2,3	3
'''
# -- lateral view outer结果：
'''
pid	    svs	   sv
1001	1,2,3	1
1001	1,2,3	2
1001	1,2,3	3
1002	NULL	NULL
'''
# 方式1和方式2使用lateral view和lateral view outer效果不同，lateral view空缺值数据丢失，lateral view outer空缺值显示为NULL

# 5）json_tuple(json_str,k1,k2,…)
# 功能：从json字符串中根据key获取对应的value返回

# 6）parse_url_tuple(url,p1,p2,…)
# 功能：从url中根据属性property获取对应的value返回
'''
select parse_url_tuple('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST', 'PATH', 'QUERY', 'REF', 'PROTOCOL', 'QUERY:k1', 'QUERY:k2')
'''
'''
c0	            c1	            c2	        c3	    c4	    c5	c6
facebook.com	/path1/p.php	k1=v1&k2=v2	Ref1	http	v1	v2
'''
# 参数详解见：https://help.aliyun.com/zh/maxcompute/user-guide/parse-url-tuple

# 7）inline(array<struct>)
# 功能：将结构体数组并列分解为多行
'''
select inline(array(struct('A',18,date '2023-10-01'),struct('B',20,date '2023-11-01'))) as (col1,col2,col3)
'''
'''
col1  col2	      col3
A	   18	2023-10-01
B	   20	2023-11-01
'''





