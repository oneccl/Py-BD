"""
Created with PyCharm.
Author: CC
E-mail: 203717588@qq.com
Date: 2023/12/12
Time: 21:13
Description:
"""

# 截取Hive数组中的前N个元素

'''
select array(1,2,3,4)            -- [1,2,3,4]
select array(1,2,3,NULL)         -- [1,2,3,null]
select split('123', space(0))    -- [1,2,3,]
'''
# 方式1：使用索引逐个取值，再收集到新的数组
'''
with t as (select split('123', space(0)) as arr)
select array(arr[0],arr[1],arr[2]) from t     -- [1,2,3]

-- 如果初始数组中包含NULL，使用下标取元素时，NULL将会被放在最前面的位置
with t as (select array(1,2,3,NULL) as arr)
select array(arr[1],arr[2],arr[3]) from t     -- [1,2,3]
'''
# 方式2：使用posexplode()分解数组，过滤pos<=N，再收集到新的数组
'''
with t as (select split('123', space(0)) as arr)
select collect_list(tmp.ele) from t
lateral view outer posexplode(arr) tmp as pos,ele
where pos < 3
group by arr     -- [1,2,3]
'''
# 方式3：先转换为字符串，再截取特定长度后转换为数组
# 数组转换为字符串后的总长度计算公式：size(arr)+(size(arr)-1)
# 数组去除前N个元素剩余元素转换为字符串的长度计算公式：(size(arr)-N)*2
# 则需要截取的前N个元素转换成字符串的长度计算公式为：(size(arr)+(size(arr)-1))-(size(arr)-N)*2 = 2*N-1
'''
with t as (select split('1,2,3,4', ',') as arr)
select split(substring(concat_ws(',',arr), 0, 2*3-1), ',')
from t           -- [1,2,3]

with t as (select split('123', space(0)) as arr)
select split(substring(concat_ws(',',arr), 0, 2*3-1), ',')
from t           -- [1,2,3]
'''



