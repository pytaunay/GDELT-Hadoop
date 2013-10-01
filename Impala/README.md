Cloudera Impala data loading and usage
--------------------------------------
This directory contains:
- Files to create the aggregate database made of the historical data files and of the daily updates, using Parquet compression
- Files to pull the daily update on a daily basis and append it to the aggregate database 

Files
-----------------------------
o gdelt_create.sh: calls the download engine to get all the historical and daily updates files, upload them to HDFS, and populate the Impala database.

o create_historical.sql: need to be called only once, when creating the aggregate database
This file creates an external table based on the historical tsv files.

o create_dailyupdates.sql: need to be called only once, when creating the aggregate database
This file creates an external table based on the daily tsv files

o create_aggregate.sql: need to be called only once, when creating the aggregate database
This file creates an Impala database with Parquet compression, by compressing the data in the
historical and dailyupdate external databases. 
