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

use test;
show tables;
select * from t_usa_covid19;  --普通表t_usa_covid19

--创建一张分区表 基于count_date日期,state州进行分区
CREATE TABLE if not exists t_usa_covid19_p(
                                              county string,
                                              fips int,
                                              cases int,
                                              deaths int)
    partitioned by(count_date string,state string)
    row format delimited fields terminated by ",";

set hive.exec.dynamic.partition.mode=nonstrict;

--动态分区
insert into table t_usa_covid19_p partition (count_date,state)
select county,fips,cases,deaths,count_date,state from t_usa_covid19;

--with
with q1 as (select num,name,age from student where num = 95002)
select *
from q1;


--------------------join----------------------
--table1: 员工表
CREATE TABLE employee(
                         id int,
                         name string,
                         deg string,
                         salary int,
                         dept string
) row format delimited
    fields terminated by ',';

--table2:员工家庭住址信息表
CREATE TABLE employee_address (
                                  id int,
                                  hno string,
                                  street string,
                                  city string
) row format delimited
    fields terminated by ',';

--table3:员工联系方式信息表
CREATE TABLE employee_connection (
                                     id int,
                                     phno string,
                                     email string
) row format delimited
    fields terminated by ',';

--加载数据到表中
load data local inpath '/opt/module/hive/data/join/employee.txt' into table employee;
load data local inpath '/opt/module/hive/data/join/employee_address.txt' into table employee_address;
load data local inpath '/opt/module/hive/data/join/employee_connection.txt' into table employee_connection;

--inner join=join=两个表间加个逗号    两个表连接上的数据才会留下来


--left join 左表都留下来，有表连上的才留下来
