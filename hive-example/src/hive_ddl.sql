show databases;
use test;

create table t_archer(
    id int comment "ID",
    name string comment "英雄名称",
    hp_max int comment "最大生命",
    mp_max int comment "最大法力",
    attack_max int comment "最高物效",
    defense_max int comment "最大物防",
    attack_range string comment "攻击范围",
    role_main string comment "主要定位",
    role_assist string comment "次要定位"
)comment "王者荣耀射手信息"
row format delimited
fields terminated by "\t";

show tables;

select * from t_archer;

create table t_hot_hero_skin_price(
    id int,
    name string,
    win_rate int,
    skin_price map<string,int>
)row format delimited
fields terminated by ","  --字段之间的分隔符
collection items terminated by "-"  --集合元素之间的分隔符
map keys terminated by ":";  --map元素kv间的分隔符

select * from t_hot_hero_skin_price;

create table t_team_ace_player(
    id int,
    team_name string,
    ace_player_name string
); --没有指定row format语句 此时采用的是默认的\001作为字段的分隔符

select * from t_team_ace_player;

create table t_team_ace_player_location(
    id int,
    team_name string,
    ace_player_name string)
location '/data'; --使用location关键字指定本张表数据在hdfs上的存储路径

select * from t_team_ace_player_location;

DESCRIBE FORMATTED test.t_team_ace_player_location;  --获取表的描述信息

-------------外部表关键字external--------------
create external table student_ext(
    num int,
    name string,
    sex string,
    age int,
    dept string)
row format delimited
fields terminated by ','
location '/stu';


-------------分区表----------------
create table t_all_hero(
    id int,
    name string,
    hp_max int,
    mp_max int,
    attack_max int,
    defense_max int,
    attack_range string,
    role_main string,
    role_assist string
)
row format delimited
fields terminated by "\t";

show tables;

--查询role_main主要定位是射手并且hp_max最大生命大于6000的有几个  很慢
select count(*) from t_all_hero where role_main="archer" and hp_max >6000;

create table t_all_hero_part(
    id int,
    name string,
    hp_max int,
    mp_max int,
    attack_max int,
    defense_max int,
    attack_range string,
    role_main string,
    role_assist string
) partitioned by (role string)--注意哦 这里是分区字段，不可以是表中已经存在的字段
row format delimited
fields terminated by "\t";

--静态加载分区表数据  文件的位置是Hive服务器所在机器本地文件系统
load data local inpath '/opt/module/hive/data/archer.txt' into table t_all_hero_part partition(role='sheshou');
load data local inpath '/opt/module/hive/data/assassin.txt' into table t_all_hero_part partition(role='cike');
load data local inpath '/opt/module/hive/data/mage.txt' into table t_all_hero_part partition(role='fashi');
load data local inpath '/opt/module/hive/data/support.txt' into table t_all_hero_part partition(role='fuzhu');
load data local inpath '/opt/module/hive/data/tank.txt' into table t_all_hero_part partition(role='tanke');
load data local inpath '/opt/module/hive/data/warrior.txt' into table t_all_hero_part partition(role='zhanshi');

select * from t_all_hero_part;

--非分区表 全表扫描过滤查询
select count(*) from t_all_hero where role_main="archer" and hp_max >6000;
--分区表 先基于分区过滤 再查询
select count(*) from t_all_hero_part where role="sheshou" and hp_max >6000;

--多重分区
show tables;
drop table t_user_province_city;
create table t_user_province_city (id int, name string,age int,city string) partitioned by (province string, city1 string);
load data local inpath '/opt/module/hive/data/user.txt' into table t_user_province_city partition(province='zhejiang',city1='hangzhou');
select * from t_user_province_city where province='zhejiang' and city1='hangzhou';

-----动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
create table t_all_hero_part_dynamic(
                                        id int,
                                        name string,
                                        hp_max int,
                                        mp_max int,
                                        attack_max int,
                                        defense_max int,
                                        attack_range string,
                                        role_main string,
                                        role_assist string
) partitioned by (role string)
    row format delimited
        fields terminated by "\t";

--执行动态分区插入
insert into table t_all_hero_part_dynamic partition(role) --注意这里 分区值并没有手动写死指定
select tmp.*,tmp.role_main from t_all_hero tmp;

select * from t_all_hero_part_dynamic;

------------分桶表----------------
--1.把数据加载到普通表
CREATE TABLE t_usa_covid19(
    count_date string,
    county string,
    state string,
    fips int,
    cases int,
    deaths int
)row format delimited fields terminated by ",";

--hadoop fs -put us-covid19-counties.dat /user/hive/warehouse/test.db/t_usa_covid19

--2.建分桶表
CREATE TABLE t_usa_covid19_bucket(
                                             count_date string,
                                             county string,
                                             state string,
                                             fips int,
                                             cases int,
                                             deaths int)
    CLUSTERED BY(state) INTO 5 BUCKETS; --分桶的字段一定要是表中已经存在的字段

--3:使用insert+select语法将数据加载到分桶表中
insert into t_usa_covid19_bucket select * from t_usa_covid19;

select *
from t_usa_covid19_bucket where state="New York";

---------事务表----------
--1、开启事务配置（可以使用set设置当前session生效 也可以配置在hive-site.xml中）
set hive.support.concurrency = true; --Hive是否支持并发
set hive.enforce.bucketing = true; --从Hive2.0开始不再需要  是否开启分桶功能
set hive.exec.dynamic.partition.mode = nonstrict; --动态分区模式  非严格
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager; --
set hive.compactor.initiator.on = true; --是否在Metastore实例上运行启动线程和清理线程
set hive.compactor.worker.threads = 1; --在此metastore实例上运行多少个压缩程序工作线程。

--2、创建Hive事务表
drop table if exists trans_student;
create table trans_student(
                              id int,
                              name String,
                              age int
)clustered by (id) into 2 buckets stored as orc TBLPROPERTIES('transactional'='true');
--注意 事务表创建几个要素：开启参数、分桶表、存储格式orc、表属性

insert into trans_student values(1,'Allen',20);

--update操作
update trans_student
set age = 20
where id = 1;

select * from trans_student;

describe formatted trans_student;


---------视图----------
--1、创建视图
create view v_usa_covid19 as select count_date, county,state,deaths from t_usa_covid19 limit 5;
--从已有的视图中创建视图
create view v_usa_covid19_from_view as select * from v_usa_covid19 limit 2;

--2、显示当前已有的视图
show tables;
show views;--hive v2.2.0之后支持

--3、视图的查询使用
select *
from v_usa_covid19;

--4、查看视图定义
show create table v_usa_covid19;

--5、删除视图
drop view v_usa_covid19_from_view;
--6、更改视图属性
    alter view v_usa_covid19 set TBLPROPERTIES ('comment' = 'This is a view');
--7、更改视图定义
    alter view v_usa_covid19 as  select county,deaths from t_usa_covid19 limit 2;

