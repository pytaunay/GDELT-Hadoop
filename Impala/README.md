Cloudera Impala data loading and usage
--------------------------------------
This directory contains:
- Files to create the aggregate database made of the historical data files and of the daily updates, using Parquet compression
- Files to pull the daily update on a daily basis and append it to the aggregate database 

Files
-----------------------------
* gdelt_create.sh : calls the download engine to get all the historical and daily updates files, uploads them to HDFS, and populates the Impala database.

* dl_engine.sh : a Bash download engine that is used to pull all the information from the servers at UT Dallas.
The engine assumes that the files are stored as ZIP files (.zip extension) on the server, and are all located under the same remote directory.
The engine has 3 [R]equired arguments and 4 [O]ptional arguments:
	- [R] The target directory in which to download the zip files (--targetdir | -t)
	- [R] The data directory in which the zip files will be extracted (--datadir | -d)
	- [R] The remote URL from where to download the zip files (--url | -u)
	- [R] A log file for the output of the script (--log)
	- [O] The number of retries if a file can not be downloaded at first (--nretry | -r)
	- [O] The number of processes to spawn when unzipping all the files 
	- [O] A verbose option to obtain more output from the command (--verbose | -v)
	- [O] A help message (--help | -h) 

The engine will check that the files are valid once downloaded, and will retry to download them if it failed. 

* hdfs_upload.sh : a Bash script to upload all the unzipped files to HDFS
The script has 3 [R]equired arguments and two [O]ptional arguments:
	- [R] The local directory where the unzipped files are located (--localdir)
	- [R] The HDFS directory in which the files will be uploaded (--hdfsdir)
	- [R] A log file to use (--log)
	- [O] A verbose option to obtain more output from the command (--verbose | -v)
	- [O] A help message (--help | -h) 

Usage
-----------------------------
1. Edit the file gdelt_create.sh to reflect your local Hadoop cluster configuration
List of variables to edit (\* denotes HIST, DU, or AGG, which refers to HISTorical files, Daily Updates, or AGGregated data made of both):
	- LOG: log file directory location
	- NRETRY: Number of download retries before a file is skipped 
	- NPROC: Number of processes used to unzip the data files
	- IMPALA_HOST: Host on which the Impala daemon is running. This should be a data node.
	- KERBEROS: Flag to set Kerberos authentication for the Impala scripts
	- TMPDIR: A temporary directory is set to save intermediary Impala query files. It is removed once the script has been executed.
	- DB_NAME: The database name to use. If it does not exist, it will be created.
	- DB_LOC: The location of the database on HDFS.
	- *_URL: The remote location of the data files to download.
	- *_TDIR: Directory where the zip files will be saved.
	- *_DDIR: Directory where the zip files will be uncompressed.
	- *_HDFSDIR: HDFS location of the TSV files or saved tables. 
	- *_TBNAME: Table name in the database. IMPORTANT: If the table already exists, it will be dropped !

2. Run the script gdelt_create.sh
```shell
./gdelt_create.sh
```

3. A log file will be created at the location ```shell $LOGDIR/gdelt_create.log.YYYYMMDD.HHmmss```. You can track its progress using ```shell tail -f $LOGDIR/gdelt_create.log.YYYYMMDD.HHmmss```.

