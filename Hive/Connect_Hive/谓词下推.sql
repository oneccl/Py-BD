
-- Hive谓词下推

show databases;

select * from hivecrud.s_log limit 3;

create database PredicatePushdown;

use PredicatePushdown;

show tables;

drop table a;
create table a (id string,value bigint) partitioned by (dt string);

set hive.exec.mode.local.auto=true;

insert into table a partition (dt="20231001") values
("1", 10);
insert into table a partition (dt="20231101") values
("3", 11),
("1", 8),
("2", 5);

select * from a;

drop table b;
create table b (id string,value bigint,rank bigint) partitioned by (dt string);
insert into b partition (dt="20231001") values
("1", 9, 2),
("4", 7, 1);
insert into b partition (dt="20231101") values
("3", 8, 1),
("1", 12, 1);

select * from b;

-- 执行计划：explain [EXTENDED|DEPENDENCY|REWRITE|LOGICAL|FORMATTED] select...;
-- extended：显示更详细的执行计划信息，包括每个任务的详细信息、每个阶段的运行时间和输入/输出记录数等
-- formatted：将执行计划以JSON字符串形式输出
-- logical：显示查询语句的逻辑执行计划
-- rewrite：显示查询语句的重写规则
-- dependency：显示查询语句中所有依赖的表和分区信息

select * from a left join b on a.id=b.id where a.id='1';
explain select * from a left join b on a.id=b.id where a.id='1';
explain select * from (select * from a where id='1') t left join b on t.id=b.id;
explain select * from a left join b on a.id=b.id and a.id='1';


-- full join

select * from a full join b on a.id=b.id where b.rank=1;

select * from a full join b on a.id=b.id and b.rank=1;

select * from a full join (select * from b where rank=1) t on a.id=t.id;







