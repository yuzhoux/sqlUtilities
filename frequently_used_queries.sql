-- Frequently used queries

drop table if exists table_schema.table_name;
create table table_schema.table_name
stored as textfile
location "hdfs://location"
as select col1, col2, col3, rank() over (partition by col4 order by col5 desc) as rank from table; 