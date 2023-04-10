show databases;

create database hive_sqoop;
create table emp_add(
    id int,
    hno string,
    street string,
    city string
)row format delimited
    fields terminated by ','
stored as orc;

show databases;
use hive_sqoop;
show tables;
use test;
show tables;
drop table emp_add;

-----------ODS层-------------
create database if not exists ods;
--设置写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;
use ods;

create external table if not exists ods.web_chat_ems (
    id INT comment '主键',
    create_date_time STRING comment '数据创建时间',
    session_id STRING comment '七陌sessionId',
    sid STRING comment '访客id',
    create_time STRING comment '会话创建时间',
    seo_source STRING comment '搜索来源',
    seo_keywords STRING comment '关键字',
    ip STRING comment 'IP地址',
    area STRING comment '地域',
    country STRING comment '所在国家',
    province STRING comment '省',
    city STRING comment '城市',
    origin_channel STRING comment '投放渠道',
    user_match STRING comment '所属坐席',  --字段名不要采用关键字，比如原始mysql表中有一个user字段，我们需要将它修改为user_match
    manual_time STRING comment '人工开始时间',
    begin_time STRING comment '坐席领取时间 ',
    end_time STRING comment '会话结束时间',
    last_customer_msg_time_stamp STRING comment '客户最后一条消息的时间',
    last_agent_msg_time_stamp STRING comment '坐席最后一下回复的时间',
    reply_msg_count INT comment '客服回复消息数',
    msg_count INT comment '客户发送消息数',
    browser_name STRING comment '浏览器名称',
    os_info STRING comment '系统名称')
    comment '访问会话信息表'
    PARTITIONED BY(starts_time STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress'='ZLIB');

CREATE EXTERNAL TABLE IF NOT EXISTS ods.web_chat_text_ems (
     id INT COMMENT '主键来自MySQL',
     referrer STRING comment '上级来源页面',
     from_url STRING comment '会话来源页面',
     landing_page_url STRING comment '访客着陆页面',
     url_title STRING comment '咨询页面title',
     platform_description STRING comment '客户平台信息',
     other_params STRING comment '扩展字段中数据',
     history STRING comment '历史访问记录'
) comment 'EMS-PV测试表'
PARTITIONED BY(start_time STRING)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
stored as orc
TBLPROPERTIES ('orc.compress'='ZLIB');

--------------DWD层-----------------
CREATE DATABASE IF NOT EXISTS dwd;
create table if not exists dwd.visit_consult_dwd
(
    session_id           STRING comment '七陌sessionId',
    sid                  STRING comment '访客id',
    create_time          bigint comment '会话创建时间',
    seo_source           STRING comment '搜索来源',
    ip                   STRING comment 'IP地址',
    area                 STRING comment '地域',
    msg_count            int comment '客户发送消息数',
    origin_channel       STRING COMMENT '来源渠道',
    referrer             STRING comment '上级来源页面',
    from_url             STRING comment '会话来源页面',
    landing_page_url     STRING comment '访客着陆页面',
    url_title            STRING comment '咨询页面title',
    platform_description STRING comment '客户平台信息',
    other_params         STRING comment '扩展字段中数据',
    history              STRING comment '历史访问记录',
    hourinfo             string comment '小时'
)
comment '访问咨询DWD表'
partitioned by(yearinfo String, quarterinfo string,monthinfo String, dayinfo string)
row format delimited fields terminated by '\t'
stored as orc
tblproperties ('orc.compress'='SNAPPY');

-------------DWS层-----------------
CREATE DATABASE IF NOT EXISTS dws;
----访问量统计结果表
CREATE TABLE IF NOT EXISTS dws.visit_dws (
    sid_total INT COMMENT '根据sid去重求count',
    sessionid_total INT COMMENT '根据sessionid去重求count',
    ip_total INT COMMENT '根据IP去重求count',
    area STRING COMMENT '区域信息',
    seo_source STRING COMMENT '搜索来源',
    origin_channel STRING COMMENT '来源渠道',
    hourinfo STRING COMMENT '创建时间，统计至小时',
    time_str STRING COMMENT '时间明细',
    from_url STRING comment '会话来源页面',
    groupType STRING COMMENT '产品属性类型：1.地区；2.搜索来源；3.来源渠道；4.会话来源页面；5.总访问量',
    time_type STRING COMMENT '时间聚合类型：1、按小时聚合；2、按天聚合；3、按月聚合；4、按季度聚合；5、按年聚合；')
comment 'EMS访客日志dws表'
PARTITIONED BY(yearinfo STRING,quarterinfo STRING,monthinfo STRING,dayinfo STRING)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
TBLPROPERTIES ('orc.compress'='SNAPPY');

----咨询量统计结果表
CREATE TABLE IF NOT EXISTS dws.consult_dws
(
    sid_total INT COMMENT '根据sid去重求count',
    sessionid_total INT COMMENT '根据sessionid去重求count',
    ip_total INT COMMENT '根据IP去重求count',
    area STRING COMMENT '区域信息',
    origin_channel STRING COMMENT '来源渠道',
    hourinfo STRING COMMENT '创建时间，统计至小时',
    time_str STRING COMMENT '时间明细',
    groupType STRING COMMENT '产品属性类型：1.地区；2.来源渠道',
    time_type STRING COMMENT '时间聚合类型：1、按小时聚合；2、按天聚合；3、按月聚合；4、按季度聚合；5、按年聚合；'
)
COMMENT '咨询量DWS宽表'
PARTITIONED BY (yearinfo string, quarterinfo STRING, monthinfo STRING, dayinfo string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');


select count(1) from ods.web_chat_ems;

--将ODS层的数据经过转换+维度退化到DWD层
--动态分区配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;

set hive.auto.convert.join= false;

insert into table dwd.visit_consult_dwd partition (yearinfo, quarterinfo, monthinfo, dayinfo)
select wce.session_id,
       wce.sid,
       unix_timestamp(wce.create_time) as create_time,
       wce.seo_source,
       wce.ip,
       wce.area,
       wce.msg_count,
       wce.origin_channel,
       wcte.referrer,
       wcte.from_url,
       wcte.landing_page_url,
       wcte.url_title,
       wcte.platform_description,
       wcte.other_params,
       wcte.history,
       substr(wce.create_time, 12, 2) as hourinfo,
       substr(wce.create_time, 1, 4) as yearinfo,
       quarter(wce.create_time) as quarterinfo,
       substr(wce.create_time, 6, 2) as monthinfo,
       substr(wce.create_time, 9, 2) as dayinfo
from ods.web_chat_ems wce inner join ods.web_chat_text_ems wcte on wce.id = wcte.id;


---统计每年的总访问量
insert into dws.visit_dws partition(yearinfo, quarterinfo, monthinfo, dayinfo)
select count(distinct sid) as sid_total,
       count(distinct session_id) as session_total,
       count(distinct ip) as ip_total,
       '-1' as area,
       '-1' as seo_source,
       '-1' as origin_channel,
       yearinfo as time_str,
       '-1' as from_url,
       '5' as groupType,
       '5' as time_type,
       yearinfo,
       '-1' as quarterinfo,
       '-1' as monthinfo,
       '-1' as dayinfo
from dwd.visit_consult_dwd
group by yearinfo;

insert into dws.consult_dws partition(yearinfo, quarterinfo, monthinfo, dayinfo)
select count(distinct sid) as sid_total,
       count(distinct session_id) as session_total,
       count(distinct ip) as ip_total,
       '-1' as area,
       '-1' as origin_channel,
       '-1' as hourinfo,
       yearinfo as time_str,
       '3' as groupType,
       '5' as time_type,
       yearinfo,
       '-1' as quarterinfo,
       '-1' as monthinfo,
       '-1' as dayinfo
from dwd.visit_consult_dwd
where msg_count>=1
group by yearinfo;


