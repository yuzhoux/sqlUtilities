-- Frequently used queries

-- create table by extracting from other tables
drop table if exists table_schema.table_name;
create table table_schema.table_name
stored as textfile
location "hdfs://location"
as select col1, col2, col3, 
case when col4 = 'cond1' then 1
when col4 = 'cond2' then 2
else 0 end as col4_number
,row_number() over (partition by col4 order by col5 desc) as rowNum from table; 

-- create table by loading data from files
drop table if exists table_schema.table_name;
create table table_schema.table_name (col1 string, col2 int, col3 string)
row format delimited fields terminated by ','
stored as textfile
location "hdfs://location";

load data inpath "hdfs://location/file.txt" overwrite into table table_schema.table_name;

-- using With clause to create temp tables
drop table if exists table_schema.table_name;
create table table_schema.table_name
stored as textfile
location "hdfs://location"
as 
with temp1 as (select col1,col2 from table1),
temp2 as (select col3,col4 from table2)
select temp1.col1, temp1.col2, temp2.col4 from temp1 left join temp2 on temp1.col1=temp2.col3;

-- create table with partitions and insert into a partition
drop table if exists table_schema.table_name;
create table table_schema.table_name (col1 string, col2 int, col3 string)
partitioned by (month string)
row format delimited fields terminated by ','
stored as textfile
location "hdfs://location";

alter table table_schema.table_name drop if exists partition(month='2018-07');

insert into table_schema.table_name partition(month='2018-07')
select col1, col2, col3, count(1) as CNT,
sum(case when col5>0 then col6 else 0 end) as Total from table group by col4;

-- work with date
drop table if exists table_schema.table_name;
create table table_schema.table_name
stored as textfile
location "hdfs://location"
as select from_unixtime(unix_timestamp(dt,'yyyyMMdd'),'yyy-MM-dd') as date,
cast(date_sub(cast(from_unixtime(dt,'yyyyMMdd','yyy-MM-dd') as timestamp),interval 13 months) as string) as begin_date
from table;

