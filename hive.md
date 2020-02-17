```
create external table name-database (
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
