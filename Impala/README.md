Cloudera Impala data loading and usage
--------------------------------------
This directory contains:
- Files to create the aggregate database made of the historical data files and of the daily updates, using Parquet compression
- Files to pull the daily update on a daily basis and append it to the aggregate database 

Files
-----------------------------
- gdelt_create.sh : calls the download engine to get all the historical and daily updates files, upload them to HDFS, and populate the Impala database.

- dl_engine.sh : a Bash download engine that is used to pull all the information from the servers at UT Dallas.
The engine assumes that the files are stored as ZIP files (.zip extension) on the server, and are all located under the same remote directory.
The engine has 3 [R]equired arguments and 4 [O]ptional arguments:
1. [R] The target directory in which to download the zip files (--targetdir | -t)
2. [R] The data directory in which the zip files will be extracted (--datadir | -d)
3. [R] The remote URL from where to download the zip files (--url | -u)
4. [R] A log file for the output of the script (--log)
5. [O] The number of retries if a file can not be downloaded at first (--nretry | -r)
6. [O] The number of processes to spawn when unzipping all the files 
7. [O] A verbose option to obtain more output from the command (--verbose | -v)
8. [O] A help message (--help | -h) 

The engine will check that the files are valid once downloaded, and will retry to download them if it failed. 

- hdfs_upload.sh : a Bash script to upload all the unzipped files to HDFS
The script has 3 [R]equired arguments and two [O]ptional arguments:
1. [R] The local directory where the unzipped files are located (--localdir)
2. [R] The HDFS directory in which the files will be uploaded (--hdfsdir)
3. [R] A log file to use (--log)
4. [O] A verbose option to obtain more output from the command (--verbose | -v)
5. [O] A help message (--help | -h) 
