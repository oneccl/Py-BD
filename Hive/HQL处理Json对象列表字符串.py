"""
Created with PyCharm.
Author: CC
E-mail: 203717588@qq.com
Date: 2023/9/9
Time: 16:06
Description:
"""

# Json对象列表字符串处理

# 原始数据
'''
id(string)  info_list(string)
100         [{"name": "A", "age": 21, "addr": "CN"}, {"name": "B", "age": 20}]
101         [{"name": "C", "age": 18, "addr": "US"}]
102         NULL
'''
# 1）Json对象列表字符串解析为数组
'''
select
split(regexp_replace(regexp_replace(info_list, '\\[|\\]',''), '\\}\\, \\{', '\\}\\;\\{'), '\\;') arr
from table_name
'''
# 解释：先将字符串列表中的"["和"]"去除；再将字符串中的"}, {"替换为"};{"；最后再使用";"分割

# 2）数组列转行
# UDTF不允许在select存在多个字段：如果需要保留其他字段，需要使用LATERAL VIEW
# LATERAL VIEW子句与生成器函数（如explode）结合使用，将生成包含一个或多个行的虚拟表。LATERAL VIEW将行应用于每个原始输出行
'''
语法：LATERAL VIEW [OUTER] generator_function [table_identifier] AS column_identifier
- OUTER：如果指定了OUTER，则输入数组/映射为空或为NULL时返回NULL
- generator_function：生成器函数，如explode、split等
- table_identifier：generator_function的别名(View)，可选
- column_identifier：列别名，generator_function可用于输出行
'''
# 注意：列标识符的数目必须与generator_function函数返回的列数一致

'''
select m.id, n.json_str from (
    select *,
    split(regexp_replace(regexp_replace(info_list, '\\[|\\]',''), '\\}\\, \\{', '\\}\\;\\{'), '\\;') arr
    from table_name
) m
lateral view explode(m.arr) n as json_str
'''
'''explode()转换后，Array中为NULL的数据丢失
id(string)  json_str(string)
100         {"name": "A", "age": 21, "addr": "CN"}
100         {"name": "B", "age": 20}
101         {"name": "C", "age": 18, "addr": "US"}
'''
# 3）解析json字符串
# 方式1：json_tuple(json_str, field1, field2, ...)：若某个field在json_str中不存在，则显示为NULL
# 只能解析json字符串中都是使用""双引号包裹的字段和值
'''
select m.id, n.json_str, p.name, p.age, p.addr from (
    select *,
    split(regexp_replace(regexp_replace(info_list, '\\[|\\]',''), '\\}\\, \\{', '\\}\\;\\{'), '\\;') arr
    from table_name
) m
lateral view explode(m.arr) n as json_str
lateral view json_tuple(n.json_str, 'name', 'age', 'addr') p as name,age,addr
'''
'''
id(string)  json_str(string)                          name    age     addr
100         {"name": "A", "age": 21, "addr": "CN"}    A       21      CN
100         {"name": "B", "age": 20}                  B       20      NULL
101         {"name": "C", "age": 18, "addr": "US"}    C       18      US
'''

# 方式2：get_json_object(json_str, field_path)：
# 基本示例：
'''
select get_json_object('{"arr": [{"name":"A","age":25}, {"name":"B","age":20}]}', "$.arr[0].age")
select get_json_object("{'arr': [{'name': 'B'}, {'name': 'B','age': 20}]}", '$.arr[1].age')
select get_json_object('{"name": "A", "age": 25}', "$.age")
select get_json_object('{"p1": {"sex": "男", "age": 18}, "p2": "null"}', "$.p1.age")
'''
# 可以解析json字符串中都是使用""双引号或''单引号包裹的字段和值
'''
select m.id, n.json_str, 
    get_json_object(n.json_str, "$.name") name, 
    get_json_object(n.json_str, "$.age") age, 
    get_json_object(n.json_str, "$.age") addr
from (
    select *,
    split(regexp_replace(regexp_replace(info_list, '\\[|\\]',''), '\\}\\, \\{', '\\}\\;\\{'), '\\;') arr
    from table_name
) m
lateral view explode(m.arr) n as json_str
'''
'''
id(string)  json_str(string)                          name    age     addr
100         {"name": "A", "age": 21, "addr": "CN"}    A       21      CN
100         {"name": "B", "age": 20}                  B       20      NULL
101         {"name": "C", "age": 18, "addr": "US"}    C       18      US
'''
# 4）存在问题与解决
# 现象：如果Array列存在NULL，则该行不会显示（原因：explode()转换的Array中若包含NULL，则结果中不会有该行记录），最终导致数据丢失

# 解决方案：
# 方式1：使用coalesce()将NULL替换为['']，结果数据不会丢失，explode()结果显示为''
'''
select m.id, n.json_str, p.name, p.age, p.addr from (
    select *,
    -- split('',''))用于生成['']
    coalesce(split(regexp_replace(regexp_replace(info_list, '\\[|\\]',''), '\\}\\, \\{', '\\}\\;\\{'), '\\;'), split('','')) arr
    from table_name
) m
lateral view explode(m.arr) n as json_str
-- explode()结果显示为''的行通过json_tuple()解析后结果为NULL
lateral view json_tuple(n.json_str, 'name', 'age', 'addr') p as name,age,addr
'''
'''
id(string)  json_str(string)                          name    age     addr
100         {"name": "A", "age": 21, "addr": "CN"}    A       21      CN
100         {"name": "B", "age": 20}                  B       20      NULL
101         {"name": "C", "age": 18, "addr": "US"}    C       18      US
102                                                   NULL    NULL    NULL
'''
# 方式2：使用LATERAL VIEW OUTER，结果数据不会丢失，explode()结果显示为NULL
'''
select m.id, n.json_str, p.name, p.age, p.addr from (
    select *,
    split(regexp_replace(regexp_replace(info_list, '\\[|\\]',''), '\\}\\, \\{', '\\}\\;\\{'), '\\;') arr
    from table_name
) m
lateral view outer explode(m.arr) n as json_str
lateral view json_tuple(n.json_str, 'name', 'age', 'addr') p as name,age,addr
'''
'''
id(string)  json_str(string)                          name    age     addr
100         {"name": "A", "age": 21, "addr": "CN"}    A       21      CN
100         {"name": "B", "age": 20}                  B       20      NULL
101         {"name": "C", "age": 18, "addr": "US"}    C       18      US
102         NULL                                      NULL    NULL    NULL
'''

