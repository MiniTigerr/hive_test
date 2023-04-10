------insert+select-----------
--step1:创建一张源表student
create table student(num int,name string,sex string,age int,dept string)
row format delimited
fields terminated by ',';

load data local inpath '/opt/module/hive/data/students.txt' into table student;

select * from student;

--step2：创建一张目标表  只有两个字段
create table student_from_insert(sno int,sname string);
--使用insert+select插入数据到新表中
insert into table student_from_insert
select num,name from student;

select *
from student_from_insert;


--多重插入  一次扫描 多次插入
--创建两张新表
create table student_insert1(sno int);
create table student_insert2(sname string);

from student
insert overwrite table student_insert1
select num
insert overwrite table student_insert2
select name;

---------------动态分区插入--------------------
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

select * from student;

--1.创建分区表 以sdept作为分区字段
create table student_partition(Sno int,Sname string,Sex string,Sage int) partitioned by(Sdept string);

--2.执行动态分区插入操作
insert into table student_partition partition(Sdept)
select num,name,sex,age,dept from student;
--其中，num,name,sex,age作为表的字段内容插入表中
--dept作为分区字段值

show partitions student_partition;

---------------导出数据--------------------
--1、导出查询结果到HDFS指定目录下
insert overwrite directory '/tmp/hive_export/e1' select num,name,age from student limit 2;

--2、导出时指定分隔符和文件存储格式
insert overwrite directory '/tmp/hive_export/e2' row format delimited fields terminated by ','
    stored as orc
select * from student;

--3、导出数据到本地文件系统指定目录下
insert overwrite local directory '/opt/module/hive/data/hive_export/e1' select * from student;

----------------------事务------------------------


----------------------查询-------------------------

show tables;