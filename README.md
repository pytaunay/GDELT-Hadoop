GDELT-Hadoop
------------

Sets of tools and examples to create a database within Hadoop with either Hive or Impala. Provides example queries with performance results on a 4 node cluster.


Impala
-------------
Contains the original table creation and daily update routines, using Cloudera Impala

Hive
-------------
Contains the original table creation and daily update routines, using Hive 

Files
------------
dl_engine.sh: a download engine in Bash that is used to pull all the information from the servers at UT Dallas.
The engine assumes that the files are stored as ZIP files (.zip extension) on the server, and are all located under the same remote directory.
The engine has 3 required [R] arguments and 4 optional arguments [O]:
- [R] The target directory in which to download the zip files (--targetdir | -t)
- [R] The data directory in which the zip files will be extracted (--datadir | -d)
- [R] The remote URL from where to download the zip files (--url | -u)
- [O] The number of retries if a file can not be downloaded at first (--nretry | -r)
- [O] The number of processes to spawn when unzipping all the files 
- [O] A verbose option to obtain more output from the command (--verbose | -v)
- [O] A help message (--help | -h) 

The engine will check that the files are valid once downloaded, and will retry to download them if it failed. 


