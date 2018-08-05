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

-- using with to create temp tables
drop table if exists table_schema.table_name;
create table table_schema.table_name
stored as textfile
location "hdfs://location"
as 
with temp1 as (select col1,col2 from table1),
temp2 as (select col3,col4 from table2)
select temp1.col1, temp1.col2, temp2.col4 from temp1 left join temp2 on temp1.col1=temp2.col3;
