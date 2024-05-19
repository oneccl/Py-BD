"""
Created with PyCharm.
Author: CC
E-mail: 203717588@qq.com
Date: 2023/12/12
Time: 21:19
Description:
"""
# SQL数列

# Hive、MySQL、Oracle内建函数对照表：
# https://help.aliyun.com/zh/maxcompute/user-guide/mappings-between-built-in-functions-of-maxcompute-and-built-in-functions-of-hive-mysql-and-oracle
# 相关函数参考MaxCompute：
# https://help.aliyun.com/zh/maxcompute/user-guide/overview/?spm=a2c4g.11186623.0.0.738048b99fbTqb

# 简单递增序列：0,1,2,3,...

# 0~n递增序列的SQL表示公式：
'''
select
    t.pos as a_n
from (
    select posexplode(split(space(n), space(1)))
) t
'''
# 例如，生成序列0,1,2,3的SQL表示如下：
'''
select
    t.pos as a_n
from (
    select posexplode(split(space(3), space(1)))
) t
'''
'''
a_n
0
1
2
3
'''
# 生成一个递增序列三个步骤：
# 1）生成一个长度合适的数组，数组中的元素不需要具有实际含义
# 2）通过UDTF函数posexplode对数组中的每个元素生成索引下标
# 3）取出每个元素的索引下标


# 等差数列
# 定义：设首项a1，公差为d，则等差数列的通项公式为：an=a1+(n-1)d

# 等差数列的SQL表示公式：
'''
select
    a + t.pos * d as a_n
from (
    select posexplode(split(space(n - 1), space(1)))
) t
'''
# 例如，首项a1=1，公差d=2，则等差数列前三项为：
'''
select
    1 + t.pos * 2 as a_n
from (
    select posexplode(split(space(3 - 1), space(1)))
) t
'''
'''
a_n
1
3
5
'''

# 等比数列
# 定义：设首项a1，公比为q，则等比数列的通项公式为：an=a1q(n-1)

# 等比数列的SQL表示公式：
'''
select
    a * pow(q, t.pos) as a_n
from (
    select posexplode(split(space(n - 1), space(1)))
) t
'''
# 例如，首项a1=1，公比q=2，则等比数列前三项为：
'''
select
    cast(1 * pow(2, t.pos) as bigint) as a_n
from (
    select posexplode(split(space(3 - 1), space(1)))
) t
'''
'''
a_n
1
2
4
'''

# SQL数列的应用

# 连续问题

# 场景描述：蚂蚁森林用户领取的低碳排放量日数据如下
'''
with user_log as (
    select stack (
        10,
        '1001', '2023-11-11', 123,
        '1002', '2023-11-12', 145,
        '1001', '2023-11-12', 143,
        '1001', '2023-11-13', 154,
        '1003', '2023-11-11', 212,
        '1002', '2023-11-14', 168,
        '1001', '2023-11-14', 230,
        '1002', '2023-11-15', 205,
        '1003', '2023-11-12', 223,
        '1003', '2023-11-13', 201
    )
    -- 字段：用户，日期，低碳排放量
    as (id, dt, lowcarbon)
)
select * from user_log
'''
# 需求描述：计算连续登录天数大于等于3天的用户及最大登录天数
'''
select id,max_days from (
    -- 最大登录天数
    select id,sum(flag) max_days from (
        -- 连续登录天数判断：采用等差数列思想，如果差相同则连续
        select *, if(datediff(add_dt,tmp_dt)=0,1,0) flag from (
            select
                *, 
                lag(dt,1,date_sub(dt,1)) over(partition by id order by dt asc) tmp_dt,
                date_add(dt,-1) add_dt
            from user_log
        ) t1
    ) t2 group by id
) t3 where max_days >= 3
'''

# 多维分析

# 场景描述：某服务用户访问日志数据如下，每一行数据表示一条用户访问日志
'''
with visit_log as (
    select stack (
        6,
        '2023-11-01', '101', '湖北', '武汉', 'Android',
        '2023-11-01', '102', '湖南', '长沙', 'IOS',
        '2023-11-01', '103', '四川', '成都', 'Windows',
        '2023-11-02', '101', '湖北', '孝感', 'Mac',
        '2023-11-02', '102', '湖南', '邵阳', 'Android',
        '2023-11-03', '101', '湖北', '武汉', 'IOS'
    ) 
    -- 字段：日期，用户，省份，城市，设备类型
    as (dt, user_id, province, city, device_type)
)
select * from visit_log
'''
# 需求描述：计算省份，省份和城市，省份和城市和设备类型三种组合维度下的用户访问数

# 分析：针对省份province、城市city、设备类型device_type三个维度列，可以通过grouping sets聚合统计得到不同维度组合下的用户访问量

# 解决：可以借助Hive提供的GROUPING__ID来解决，核心方法是对GROUPING__ID进行逆向实现

# 1） 准备好所有的GROUPING__ID
# 生成一个包含2^n个数值的递增序列，将每个数值转换为二进制字符串，并将该二进制字符串转换为Bit数组展开二进制数组的每个比特位
# 其中，n为所有维度列的数量，2^n即为所有维度组合的数量，每个序列值表示一种维度组合，使用GROUPING__ID表示
'''
with gsb as (
    -- 所有分组
    select group_id, group_bit, group_bit_arr, pe.idx, pe.bit1 from (
        select
            groups.pos as group_id, 
            -- group_bit为每个分组数值的二进制格式（字符串）
            lpad(conv(groups.pos,10,2),3,0) as group_bit, 
            -- group_bit_arr为每个分组数值的二进制格式（数组）
            split(substring(concat_ws(',',split(lpad(conv(groups.pos,10,2),3,0), space(0))), 0, 2*3-1), ',') as group_bit_arr
        from (
            -- 3个字段的组合维度有8种，grouping__id对应0~7共8个数字，生成0~7的等差数列
            -- pow()、power()返回结果为double类型，space()需要一个int类型的参数
            select posexplode(split(space(cast(pow(2,3) as int)-1), space(1)))
        ) groups
    ) tmp
    -- 将二进制的每位转成行
    lateral view outer posexplode(group_bit_arr) pe as idx,bit1
)
select * from gsb 
'''
# 2） 准备好所有维度名称
# 生成一个字符串序列， 依次保存n个维度列的名称
'''
with dims as (
    select posexplode(split('省份,城市,设备类型',','))
)
select * from dims
'''
# 3） 将GROUPING__ID映射到维度列名称
# 对于GROUPING__ID递增数列中的每个数值，将该数值的2进制每个比特位与维度名称序列的下标进行映射，输出所有对应比特位1的维度名称。例如：
# GROUPING__ID：4 => { 1, 0, 0 }
# 维度名称序列：{省份, 城市, 设备类型}
# 映射：{1:省份, 0:城市, 0:设备类型}
# GROUPING__ID为4的数据行聚合维度即为：省份
'''
with group_dim as (
    -- 计算每种分组对应的维度字段（group_dim）
    select group_id, concat_ws(',', collect_list(case when bit1=1 then val else null end)) as dim_name from (
        -- 所有分组
        select gsb.*, dims.val from gsb left join dims on gsb.idx=dims.pos
    ) tmp
    group by group_id
)
select
    group_dim.dim_name, province, city, device_type, visit_count
from (
    select province,city,device_type,count(1) as visit_count,grouping__id as group_id
    from visit_log
    group by province, city, device_type
    grouping sets(
        (province),
        (province, city),
        (province, city, device_type)
    )
) t
join group_dim 
on t.group_id=group_dim.group_id
order by group_dim.dim_name
'''
# 结果如下：
'''
dim_name	          province	city	    device_type	visit_count
省份	                  湖北	    NULL	    NULL	    3
省份	                  湖南	    NULL	    NULL	    2
省份	                  四川	    NULL	    NULL	    1
省份，城市	          湖北	    武汉	        NULL	    2
省份，城市	          湖南	    长沙	        NULL	    1
省份，城市	          湖南	    邵阳	        NULL	    1
省份，城市	          湖北	    孝感	        NULL	    1
省份，城市	          四川	    成都	        NULL	    1
省份，城市，设备类型	  湖北	    孝感	        Mac	        1
省份，城市，设备类型	  湖南	    长沙	        IOS	        1
省份，城市，设备类型	  湖南	    邵阳	        Android	    1
省份，城市，设备类型	  四川	    成都	        Windows	    1
省份，城市，设备类型	  湖北	    武汉	        Android	    1
省份，城市，设备类型	  湖北	    武汉	        IOS	        1
'''

# ※ SQL中使用到的一些函数如下：

# Tips1：生成指定长度的空字符串
'''
-- space(n)：返回长度为n的空字符串
select space(1)
select split(space(2), space(1))          -- [,,]
'''
# Tips2：进制转换与字符串补齐
'''
-- conv(num,from_base,to_base)：进制转换：将num从from_base进制转为to_base进制，num可为int或string类型
select conv(110, 2, 10)          -- 6
select conv(11, 10, 2)           -- 1011
-- lpad(str,len,pad)：为str在左边填充pad补齐len位，返回string类型
select lpad('10', 4, 0)          -- 0010
select rpad('10', 4, 0)          -- 1000

-- 示例：将7转为2进制，使用0在左边补齐4位
select lpad(conv(7,10,2),4,0)    -- 0111
'''
# Tips3：截取数组前N个元素：见 截取数组前N个元素.py

# Tips4：Hive内置表生成函数：见 Hive内置UDTF.py

# Tips5：Hive多维分析函数：见 Hive增强GROUP BY.py






