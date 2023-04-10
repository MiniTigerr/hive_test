describe function extended concat_ws;

------------String Functions 字符串函数------------
select concat("angela","baby");
--带分隔符字符串连接函数：concat_ws(separator, [string | array(string)]+)
select concat_ws('.', 'www', array('itcast', 'cn'));
--字符串截取函数：substr(str, pos[, len]) 或者  substring(str, pos[, len])
select substr("angelababy",-2); --pos是从1开始的索引，如果为负数则倒着数
select substr("angelababy",2,2);
--正则表达式替换函数：regexp_replace(str, regexp, rep)
select regexp_replace('100-200', '(\\d+)', 'num');
--正则表达式解析函数：regexp_extract(str, regexp[, idx]) 提取正则匹配到的指定组内容
select regexp_extract('100-200', '(\\d+)-(\\d+)', 2);
--URL解析函数：parse_url 注意要想一次解析出多个 可以使用parse_url_tuple这个UDTF函数
select parse_url('http://www.itcast.cn/path/p1.php?query=1', 'HOST');
--分割字符串函数: split(str, regex)
select split('apache hive', '\\s+');
--json解析函数：get_json_object(json_txt, path)
--$表示json对象
select get_json_object('[{"website":"www.itcast.cn","name":"allenwoon"}, {"website":"cloud.itcast.com","name":"carbondata 中文文档"}]', '$.[1].website');



--字符串长度函数：length(str | binary)
select length("angelababy");
--字符串反转函数：reverse
select reverse("angelababy");
--字符串连接函数：concat(str1, str2, ... strN)
--字符串转大写函数：upper,ucase
select upper("angelababy");
select ucase("angelababy");
--字符串转小写函数：lower,lcase
select lower("ANGELABABY");
select lcase("ANGELABABY");
--去空格函数：trim 去除左右两边的空格
select trim(" angelababy ");
--左边去空格函数：ltrim
select ltrim(" angelababy ");
--右边去空格函数：rtrim
select rtrim(" angelababy ");
--空格字符串函数：space(n) 返回指定个数空格
select space(4);
--重复字符串函数：repeat(str, n) 重复str字符串n次
select repeat("angela",2);
--首字符ascii函数：ascii
select ascii("angela");  --a对应ASCII 97
--左补足函数：lpad
select lpad('hi', 5, '??');  --???hi
select lpad('hi', 1, '??');  --h
--右补足函数：rpad
select rpad('hi', 5, '??');
--集合查找函数: find_in_set(str,str_array)
select find_in_set('a','abc,b,ab,c,def');




----------- Date Functions 日期函数 -----------------
--获取当前日期: current_date
select current_date();
--获取当前时间戳: current_timestamp
--同一查询中对current_timestamp的所有调用均返回相同的值。
select current_timestamp();
--获取当前UNIX时间戳函数: unix_timestamp
select unix_timestamp();
--日期转UNIX时间戳函数: unix_timestamp
select unix_timestamp("2011-12-07 13:01:03");
--指定格式日期转UNIX时间戳函数: unix_timestamp
select unix_timestamp('20111207 13:01:03','yyyyMMdd HH:mm:ss');
--UNIX时间戳转日期函数: from_unixtime
select from_unixtime(1618238391);
select from_unixtime(0, 'yyyy-MM-dd HH:mm:ss');
--日期比较函数: datediff  日期格式要求'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'
select datediff('2012-12-08','2012-05-09');
--日期增加函数: date_add
select date_add('2012-02-28',10);
--日期减少函数: date_sub
select date_sub('2012-01-1',10);


--抽取日期函数: to_date
select to_date('2009-07-30 04:17:52');
--日期转年函数: year
select year('2009-07-30 04:17:52');
--日期转月函数: month
select month('2009-07-30 04:17:52');
--日期转天函数: day
select day('2009-07-30 04:17:52');
--日期转小时函数: hour
select hour('2009-07-30 04:17:52');
--日期转分钟函数: minute
select minute('2009-07-30 04:17:52');
--日期转秒函数: second
select second('2009-07-30 04:17:52');
--日期转周函数: weekofyear 返回指定日期所示年份第几周
select weekofyear('2009-07-30 04:17:52');




----Mathematical Functions 数学函数-------------
--取整函数: round  返回double类型的整数值部分 （遵循四舍五入）
select round(3.1415926);
--指定精度取整函数: round(double a, int d) 返回指定精度d的double类型
select round(3.1415926,4);
--向下取整函数: floor
select floor(3.1415926);
select floor(-3.1415926);
--向上取整函数: ceil
select ceil(3.1415926);
select ceil(-3.1415926);
--取随机数函数: rand 每次执行都不一样 返回一个0到1范围内的随机数
select rand();
--指定种子取随机数函数: rand(int seed) 得到一个稳定的随机数序列
select rand(3);



--二进制函数:  bin(BIGINT a)
select bin(18);
--进制转换函数: conv(BIGINT num, int from_base, int to_base)
select conv(17,10,16);
--绝对值函数: abs
select abs(-3.9);


-------Collection Functions 集合函数--------------
--集合元素size函数: size(Map<K.V>) size(Array<T>)
select size(`array`(11,22,33));
select size(`map`("id",10086,"name","zhangsan","age",18));

--取map集合keys函数: map_keys(Map<K.V>)
select map_keys(`map`("id",10086,"name","zhangsan","age",18));

--取map集合values函数: map_values(Map<K.V>)
select map_values(`map`("id",10086,"name","zhangsan","age",18));

--判断数组是否包含指定元素: array_contains(Array<T>, value)
select array_contains(`array`(11,22,33),11);
select array_contains(`array`(11,22,33),66);

--数组排序函数:sort_array(Array<T>)
select sort_array(`array`(12,2,32));



-----Conditional Functions 条件函数------------------
--使用之前课程创建好的student表数据
select * from student limit 3;

describe function extended isnull;

--if条件判断: if(boolean testCondition, T valueTrue, T valueFalseOrNull)
select if(1=2,100,200);
select if(sex ='男','M','W') from student limit 3;

--空判断函数: isnull( a )
select isnull("allen");
select isnull(null);

--非空判断函数: isnotnull ( a )
select isnotnull("allen");
select isnotnull(null);

--空值转换函数: nvl(T value, T default_value)
select nvl("allen","itcast");
select nvl(null,"itcast");

--非空查找函数: COALESCE(T v1, T v2, ...)
--返回参数中的第一个非空值；如果所有值都为NULL，那么返回NULL
select COALESCE(null,11,22,33);
select COALESCE(null,null,null,33);
select COALESCE(null,null,null);

--条件转换函数: CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END
select case 100 when 50 then 'tom' when 100 then 'mary' else 'tim' end;
select case sex when '男' then 'male' else 'female' end from student limit 3;

--nullif( a, b ):
-- 如果a = b，则返回NULL，否则返回一个
select nullif(11,11);
select nullif(11,12);

--assert_true(condition)
--如果'condition'不为真，则引发异常，否则返回null
SELECT assert_true(11 >= 0);
SELECT assert_true(-1 >= 0);




----Type Conversion Functions 类型转换函数-----------------
--任意数据类型之间转换:cast
select cast(12.14 as bigint);
select cast(12.14 as string);
select cast("hello" as int);




----Data Masking Functions 数据脱敏函数------------
--mask
--将查询回的数据，大写字母转换为X，小写字母转换为x，数字转换为n。
select mask("abc123DEF");
select mask("abc123DEF",'-','.','^'); --自定义替换的字母

--mask_first_n(string str[, int n]
--对前n个进行脱敏替换
select mask_first_n("abc123DEF",4);

--mask_last_n(string str[, int n])
select mask_last_n("abc123DEF",4);

--mask_show_first_n(string str[, int n])
--除了前n个字符，其余进行掩码处理
select mask_show_first_n("abc123DEF",4);

--mask_show_last_n(string str[, int n])
select mask_show_last_n("abc123DEF",4);

--mask_hash(string|char|varchar str)
--返回字符串的hash编码。
select mask_hash("abc123DEF");


----- Misc. Functions 其他杂项函数---------------

--如果你要调用的java方法所在的jar包不是hive自带的 可以使用add jar添加进来
--hive调用java方法: java_method(class, method[, arg1[, arg2..]])
select java_method("java.lang.Math","max",11,22);

--反射函数: reflect(class, method[, arg1[, arg2..]])
select reflect("java.lang.Math","max",11,22);

--取哈希值函数:hash
select hash("allen");

--current_user()、logged_in_user()、current_database()、version()

--SHA-1加密: sha1(string/binary)
select sha1("allen");

--SHA-2家族算法加密：sha2(string/binary, int)  (SHA-224, SHA-256, SHA-384, SHA-512)
select sha2("allen",224);
select sha2("allen",512);

--crc32加密:
select crc32("allen");

--MD5加密: md5(string/binary)
select md5("allen");


-------------explode-------------
select explode(`array`(11,12,14,15,34));

select explode(`map`("id",10086,"name","Allen","age",18));

--建表
create table the_nba_championship(
    team_name string,
    champion_year array<string>
)row format delimited
    fields terminated by ','
    collection items terminated by '|';

--加载数据
load data local inpath '/opt/module/hive/data/function/The_NBA_Championship.txt' into table  the_nba_championship;

--查看数据
select * from the_nba_championship;

--explode 不允许和其他字段一起select
select explode(champion_year) as year from the_nba_championship;
select team_name,explode(champion_year) as year from the_nba_championship;  --报错

--解决：侧视图lateral view
select a.team_name,count(*) as nums
from the_nba_championship a lateral view explode(champion_year) b as year
group by a.team_name
order by nums desc;


-------分桶抽样
show tables;
select * from t_usa_covid19_bucket;

-----------多字节分隔符
--1、清洗
--2、RegexSerde正则加载

-----------行列转换
show tables;

--1、多行转多列
--建表
create table row2col1(
                         col1 string,
                         col2 string,
                         col3 int
) row format delimited fields terminated by '\t';

--加载数据到表中
load data local inpath '/opt/module/hive/data/function/r2c1.txt' into table row2col1;

select *
from row2col1;

--sql最终实现
select
    col1,
    max(case col2 when 'c' then col3 else 0 end) as c,
    max(case col2 when 'd' then col3 else 0 end) as d,
    max(case col2 when 'e' then col3 else 0 end) as e
from
    row2col1
group by
    col1;

--2、多行转单列
select * from row2col1;
select concat("it","cast","And","heima");
select concat("it","cast","And",null);

select concat_ws("-","itcast","And","heima");
select concat_ws("-","itcast","And",null);

select collect_list(col1) from row2col1;
select collect_set(col1) from row2col1;



--建表
create table row2col2(
                         col1 string,
                         col2 string,
                         col3 int
)row format delimited fields terminated by '\t';

--加载数据到表中
load data local inpath '/opt/module/hive/data/function/r2c2.txt' into table row2col2;

select * from row2col2;

describe function extended concat_ws;

--最终SQL实现
select
    col1,
    col2,
    concat_ws(',', collect_list(cast(col3 as string))) as col3
from
    row2col2
group by
    col1, col2;


--3、多列转多行
select 'b','a','c'
union
select 'a','b','c'
union
select 'a','b','c';



--创建表
create table col2row1
(
    col1 string,
    col2 int,
    col3 int,
    col4 int
) row format delimited fields terminated by '\t';

--加载数据
load data local inpath '/opt/module/hive/data/function/c2r1.txt'  into table col2row1;

select *
from col2row1;

--最终实现
select col1, 'c' as col2, col2 as col3 from col2row1
UNION ALL
select col1, 'd' as col2, col3 as col3 from col2row1
UNION ALL
select col1, 'e' as col2, col4 as col3 from col2row1;


--4、单列转多行
select explode(split("a,b,c,d",","));

--创建表
create table col2row2(
                         col1 string,
                         col2 string,
                         col3 string
)row format delimited fields terminated by '\t';

--加载数据
load data local inpath '/root/hivedata/c2r2.txt' into table col2row2;

select * from col2row2;

select explode(split(col3,',')) from col2row2;

--SQL最终实现
select
    col1,
    col2,
    lv.col3 as col3
from
    col2row2 lateral view explode(split(col3, ',')) lv as col3;


--------------------------------hive json格式数据处理-------------------------------------------------------
--get_json_object
--创建表
create table tb_json_test1 (
    json string
);

--加载数据
load data local inpath '/root/hivedata/device.json' into table tb_json_test1;

select * from tb_json_test1;


select
    --获取设备名称
    get_json_object(json,"$.device") as device,
    --获取设备类型
    get_json_object(json,"$.deviceType") as deviceType,
    --获取设备信号强度
    get_json_object(json,"$.signal") as signal,
    --获取时间
    get_json_object(json,"$.time") as stime
from tb_json_test1;

--json_tuple
--单独使用
select
    --解析所有字段
    json_tuple(json,"device","deviceType","signal","time") as (device,deviceType,signal,stime)
from tb_json_test1;

--搭配侧视图使用
select
    json,device,deviceType,signal,stime
from tb_json_test1
         lateral view json_tuple(json,"device","deviceType","signal","time") b
         as device,deviceType,signal,stime;


--JsonSerDe
--创建表
create table tb_json_test2 (
                               device string,
                               deviceType string,
                               signal double,
                               `time` string
)
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    STORED AS TEXTFILE;

load data local inpath '/root/hivedata/device.json' into table tb_json_test2;

select *
from tb_json_test2;


--------------------------------hive 窗口函数应用实例-------------------------------------------------------
--1、连续登陆用户
--建表
create table tb_login(
                         userid string,
                         logintime string
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/login.log' into table tb_login;

select *
from tb_login;

--自连接过滤实现
--a.构建笛卡尔积
select
    a.userid as a_userid,
    a.logintime as a_logintime,
    b.userid as b_userid,
    b.logintime as b_logintime
from tb_login a,tb_login b;

--上述查询结果保存为临时表
create table tb_login_tmp as
select
    a.userid as a_userid,
    a.logintime as a_logintime,
    b.userid as b_userid,
    b.logintime as b_logintime
from tb_login a,tb_login b;

--过滤数据：用户id相同并且登陆日期相差1
select
    a_userid,a_logintime,b_userid,b_logintime
from tb_login_tmp
where a_userid = b_userid
  and cast(substr(a_logintime,9,2) as int) - 1 = cast(substr(b_logintime,9,2) as int);

--统计连续两天登陆用户
select
    distinct a_userid
from tb_login_tmp
where a_userid = b_userid
  and cast(substr(a_logintime,9,2) as int) - 1 = cast(substr(b_logintime,9,2) as int);


----窗口函数实现
--连续登陆2天
select
    userid,
    logintime,
    --本次登陆日期的第二天
    date_add(logintime,1) as nextday,
    --按照用户id分区，按照登陆日期排序，取下一次登陆时间，取不到就为0
    lead(logintime,1,0) over (partition by userid order by logintime) as nextlogin
from tb_login;

--实现
with t1 as (
    select
        userid,
        logintime,
        --本次登陆日期的第二天
        date_add(logintime,1) as nextday,
        --按照用户id分区，按照登陆日期排序，取下一次登陆时间，取不到就为0
        lead(logintime,1,0) over (partition by userid order by logintime) as nextlogin
    from tb_login )
select distinct userid from t1 where nextday = nextlogin;


--连续3天登陆
select
    userid,
    logintime,
    --本次登陆日期的第三天
    date_add(logintime,2) as nextday,
    --按照用户id分区，按照登陆日期排序，取下下一次登陆时间，取不到就为0
    lead(logintime,2,0) over (partition by userid order by logintime) as nextlogin
from tb_login;

--实现
with t1 as (
    select
        userid,
        logintime,
        --本次登陆日期的第三天
        date_add(logintime,2) as nextday,
        --按照用户id分区，按照登陆日期排序，取下下一次登陆时间，取不到就为0
        lead(logintime,2,0) over (partition by userid order by logintime) as nextlogin
    from tb_login )
select distinct userid from t1 where nextday = nextlogin;

--连续N天
select
    userid,
    logintime,
    --本次登陆日期的第N天
    date_add(logintime,N-1) as nextday,
    --按照用户id分区，按照登陆日期排序，取下下一次登陆时间，取不到就为0
    lead(logintime,N-1,0) over (partition by userid order by logintime) as nextlogin
from tb_login;



--2、级联累加求和
--建表加载数据
create table tb_money(
                         userid string,
                         mth string,
                         money int
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/money.tsv' into table tb_money;

select * from tb_money;



--	统计得到每个用户每个月的消费总金额
create table tb_money_mtn as
select
    userid,
    mth,
    sum(money) as m_money
from tb_money
group by userid,mth;

select * from tb_money_mtn;

--方案一：自连接分组聚合
--	基于每个用户每个月的消费总金额进行自连接
select
    a.*,b.*
from tb_money_mtn a join tb_money_mtn b on a.userid = b.userid;

--	将每个月之前月份的数据过滤出来
select
    a.*,b.*
from tb_money_mtn a join tb_money_mtn b on a.userid = b.userid
where  b.mth <= a.mth;

--	同一个用户 同一个月的数据分到同一组  再根据用户、月份排序
select
    a.userid,
    a.mth,
    max(a.m_money) as current_mth_money,  --当月花费
    sum(b.m_money) as accumulate_money    --累积花费
from tb_money_mtn a join tb_money_mtn b on a.userid = b.userid
where b.mth <= a.mth
group by a.userid,a.mth
order by a.userid,a.mth;




--方案二：窗口函数实现
--	统计每个用户每个月消费金额及累计总金额
select
    userid,
    mth,
    m_money,
    sum(m_money) over (partition by userid order by mth rows between 1 preceding and 2 following) as t_money
from tb_money_mtn;





--3、分组TopN问题
--建表加载数据

create table tb_emp(
                       empno string,
                       ename string,
                       job string,
                       managerid string,
                       hiredate string,
                       salary double,
                       bonus double,
                       deptno string
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/emp.txt' into table tb_emp;

select * from tb_emp;


--	基于row_number实现，按照部门分区，每个部门内部按照薪水降序排序
select
    empno,
    ename,
    salary,
    deptno,
    row_number() over (partition by deptno order by salary desc) as rn
from tb_emp;

--	过滤每个部门的薪资最高的前两名
with t1 as (
    select
        empno,
        ename,
        salary,
        deptno,
        row_number() over (partition by deptno order by salary desc) as rn
    from tb_emp )
select * from t1 where rn < 3;








--------------------------------hive 拉链表设计实现-------------------------------------------------------
--1、建表加载数据
--创建拉链表
create table dw_zipper(
                          userid string,
                          phone string,
                          nick string,
                          gender int,
                          addr string,
                          starttime string,
                          endtime string
) row format delimited fields terminated by '\t';

--加载模拟数据
load data local inpath '/root/hivedata/zipper.txt' into table dw_zipper;
--查询
select userid,nick,addr,starttime,endtime from dw_zipper;


--	创建ods层增量表 加载数据
create table ods_zipper_update(
                                  userid string,
                                  phone string,
                                  nick string,
                                  gender int,
                                  addr string,
                                  starttime string,
                                  endtime string
) row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/update.txt' into table ods_zipper_update;

select * from ods_zipper_update;


--合并数据
--创建临时表
create table tmp_zipper(
                           userid string,
                           phone string,
                           nick string,
                           gender int,
                           addr string,
                           starttime string,
                           endtime string
) row format delimited fields terminated by '\t';

--	合并拉链表与增量表
insert overwrite table tmp_zipper
select
    userid,
    phone,
    nick,
    gender,
    addr,
    starttime,
    endtime
from ods_zipper_update
union all
--查询原来拉链表的所有数据，并将这次需要更新的数据的endTime更改为更新值的startTime
select
    a.userid,
    a.phone,
    a.nick,
    a.gender,
    a.addr,
    a.starttime,
    --如果这条数据没有更新或者这条数据不是要更改的数据，就保留原来的值，否则就改为新数据的开始时间-1
    if(b.userid is null or a.endtime < '9999-12-31', a.endtime , date_sub(b.starttime,1)) as endtime
from dw_zipper a  left join ods_zipper_update b
                            on a.userid = b.userid ;



--	覆盖拉链表
insert overwrite table dw_zipper
select * from tmp_zipper;