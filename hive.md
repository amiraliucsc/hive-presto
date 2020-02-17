Example of how to create an external table in Hive for this sample-data. 


```sql
create external table table-name (
  gender string,
  year int,
  name string,
  count int)
partitioned by (state string)
ROW FORMAT DELIMITED
  fields terminated by ','
  escaped by '\\'
  lines terminated by '\n'
location 's3://bucket-name/[directory]/[directory]/';
```
