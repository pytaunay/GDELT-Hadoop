#!/bin/bash

# Name: gdelt_create.sh
# Author: Pierre-Yves Taunay (py [dot] taunay [at] psu [dot] edu)
# Description: sets up the environment for downloading the GDELT database, uploading it to HDFS, and setting up
# an Impala database with or without partitioning.

##########################################################
## 	       EDIT ALL VARIABLES BELOW			##
##########################################################
###################
## Log directory location
LOGDIR=log

###################
## Download setups
# Number of retries before skipping the download of a zip file
NRETRY=10
# Number of processes to spawn when unzipping the files
NPROC=1

###################
## Impala setup
# Location of the Impala daemon
IMPALA_HOST=myimpalahost
# Kerberos authentication ? 1 = Yes, 0 = No
KERBEROS=0
# Temporary directory for the intermediate SQL files
TMPDIR=/tmp/gdelt_create
# Database name to use
DB_NAME=mydatabase
# Database location
DB_LOC=/hdfs/path/to/database/root/dir
# Use partioning by year ? 
# Partitioning by year is more resource intensive when doing INSERTs but results
# in faster queries when considering each year separately.
# Other partitioning schemes can be considered, such as by year AND month, or even days, or also
# by actors (?), though partitions should be ~1G in size for parquet compression to be efficient.
PARTITION=0

###################
## GDELT Setup
# Remote location of the historical backfiles
HIST_URL=http://gdelt.utdallas.edu/data/backfiles/
# Local location of the zip files for the historical backfiles
HIST_TDIR=/local/path/to/historical/zip
# Local location of the unzipped tsv files for the historical backfiles
HIST_DDIR=/local/path/to/historical/unzip
# HDFS location of the unzipped tsv files for the historical backfiles
HIST_HDFSDIR=/hdfs/path/to/historical
# Table name in the Impala database. If the table exists, it will be dropped !
HIST_TBNAME=gdelt_historical_raw

# Remote location of the daily updates files 
DU_URL=http://gdelt.utdallas.edu/data/dailyupdates/
# Local location of the zip files for the daily updates 
DU_TDIR=/local/path/to/dailyupdates/zip
# Local location of the unzipped tsv files for the daily updates files
DU_DDIR=/local/path/to/dailyupdates/unzip
# HDFS location of the unzipped tsv files for the daily updates files 
DU_HDFSDIR=/hdfs/path/to/dailyupdates
# Table name in the Impala database. If the table exists, it will be dropped !
DU_TBNAME=gdelt_dailyupdates_raw

# HDFS location of the aggregated Impala table
AGG_HDFSDIR=/hdfs/path/to/aggregated
# Table name in the Impala database for the aggregated data (historical + daily updates). If the table exists, it will be dropped !
AGG_TBNAME=GDELT

##########################################################
## 		 SCRIPT STARTS HERE 			##
##########################################################
EXT=`date +%Y%m%d.%H%M%S`
LOG=gdelt_create.log.$EXT

if [ ! -d "$LOGDIR" ];
then
	mkdir -p $LOGDIR &> /dev/null
	OUT=$?
	if ! [[ $OUT -eq 0 ]];
	then
		echo "ERROR Could not create $LOGDIR (Error $OUT -- Wrong permissions ?)"
		exit 1;
	fi
fi	
LOG=$LOGDIR/$LOG
touch $LOG

##################################
## Historical data set raw data ##
##################################
# Download the data
#./dl_engine.sh --targetdir $HIST_TDIR --datadir $HIST_DDIR --url $HIST_URL --nretry $NRETRY --nproc $NPROC -v --log $LOG 
OUT=$?
if ! [[ $OUT -eq 0 ]];
then
	echo "ERROR Historical data DL Engine failed. Check $LOG"
	exit 1
fi	

# Rename the files 
echo "Renaming files to .tsv files..." | tee -a $LOG
rename .csv .tsv $HIST_DDIR/*
echo "...done" | tee -a $LOG

# Upload to HDFS
./hdfs_upload.sh --localdir $HIST_DDIR --hdfsdir $HIST_HDFSDIR --log $LOG -v 
OUT=$?
if ! [[ $OUT -eq 0 ]];
then
	echo "ERROR Historical data HDFS upload failed. Check $LOG."
	exit 1
fi	

#####################################
## Daily updates data set raw data ##
#####################################
# Download the data
echo "------------------------------------" | tee -a $LOG
#./dl_engine.sh --targetdir $DU_TDIR --datadir $DU_DDIR --url $DU_URL --nretry $NRETRY --nproc $NPROC -v --log $LOG
OUT=$?
if ! [[ $OUT -eq 0 ]];
then
	echo "ERROR Daily update data DL Engine failed. Check $LOG"
	echo "------------------------------------" | tee -a $LOG
	exit 1
fi	

echo "------------------------------------" | tee -a $LOG
echo "Renaming files to .tsv files..." | tee -a $LOG
rename .CSV .tsv $DU_DDIR/*
echo "...done" | tee -a $LOG
echo "------------------------------------" | tee -a $LOG

# Upload to HDFS
./hdfs_upload.sh --localdir $DU_DDIR --hdfsdir $DU_HDFSDIR --log $LOG -v 
OUT=$?
if ! [[ $OUT -eq 0 ]];
then
	echo "ERROR Daily update data HDFS upload failed. Check $LOG."
	echo "------------------------------------" | tee -a $LOG
	exit 1
fi	
echo "------------------------------------" | tee -a $LOG

#############################
## Create the Impala query ##
#############################
echo "Creating the Impala query ..."
# Edit the SQL files
if [ ! -d $TMPDIR ];
then
	mkdir -p $TMPDIR &> /dev/null
fi	

cp skel/hist.skel $TMPDIR/create_historical.sql
SAFEDIR=$(echo $HIST_HDFSDIR | sed 's \([]\#\%\@\*$\/&[]\) \\\1 g')
sed -i "s/DROP TABLE IF EXISTS/& $HIST_TBNAME/" $TMPDIR/create_historical.sql
sed -i "s/CREATE EXTERNAL TABLE/& $HIST_TBNAME/" $TMPDIR/create_historical.sql
sed -i "s/LOCATION/& \'$SAFEDIR\'/" $TMPDIR/create_historical.sql

cp skel/daily.skel $TMPDIR/create_dailyupdates.sql
SAFEDIR=$(echo $DU_HDFSDIR | sed 's \([]\#\%\@\*$\/&[]\) \\\1 g')
sed -i "s/DROP TABLE IF EXISTS/& $DU_TBNAME/" $TMPDIR/create_dailyupdates.sql
sed -i "s/CREATE EXTERNAL TABLE/& $DU_TBNAME/" $TMPDIR/create_dailyupdates.sql
sed -i "s/LOCATION/& \'$SAFEDIR\'/" $TMPDIR/create_dailyupdates.sql

if [ $PARTITION -eq 0 ];
then
	cp skel/aggregate.skel $TMPDIR/create_aggregate.sql
	SAFEDIR=$(echo $AGG_HDFSDIR | sed 's \([]\#\%\@\*$\/&[]\) \\\1 g')
	sed -i "s/DROP TABLE IF EXISTS/& $AGG_TBNAME/" $TMPDIR/create_aggregate.sql
	sed -i "s/CREATE TABLE LIKE STORED AS PARQUETFILE LOCATION/CREATE TABLE $AGG_TBNAME LIKE $DU_TBNAME STORED AS PARQUETFILE LOCATION \'$SAFEDIR\'/" $TMPDIR/create_aggregate.sql
	sed -i "s/INSERT OVERWRITE SELECT/INSERT OVERWRITE $AGG_TBNAME SELECT/" $TMPDIR/create_aggregate.sql
	sed -i "s/INSERT INTO SELECT/INSERT INTO $AGG_TBNAME SELECT/" $TMPDIR/create_aggregate.sql

	sed -i "s/^FROM/& $HIST_TBNAME/" $TMPDIR/create_aggregate.sql
	sed -i "s/ FROM/& $DU_TBNAME/" $TMPDIR/create_aggregate.sql
fi	

# Create the query for Impala
echo "CREATE DATABASE IF NOT EXISTS $DB_NAME LOCATION '$DB_LOC';" > create_query.sql 
echo "USE $DB_NAME;" >> create_query.sql
cat $TMPDIR/create_historical.sql $TMPDIR/create_dailyupdates.sql $TMPDIR/create_aggregate.sql >> create_query.sql
echo "...done"
echo "------------------------------------" | tee -a $LOG


##############################
## Execute the Impala query ##
##############################
echo "Executing the Impala query; patience ..." | tee -a $LOG
if [ $KERBEROS -eq 1 ];
then
	impala-shell -k -i $IMPALA_HOST -f create_query.sql &>> $LOG 
else
	impala-shell -i $IMPALA_HOST -f create_query.sql &>> $LOG 
fi	
echo "...done" | tee -a $LOG
echo "------------------------------------" | tee -a $LOG

#############################################################
## Test query to verify we have the same number of records ##
#############################################################
echo "Starting test ..." | tee -a $LOG
if [ $KERBEROS -eq 1 ];
then
	impala-shell -k -i $IMPALA_HOST -q "USE $DB_NAME; SELECT COUNT(*) FROM $HIST_TBNAME ;" -o $TMPDIR/impalatest1.1.log &>> $LOG
	impala-shell -k -i $IMPALA_HOST -q "USE $DB_NAME; SELECT COUNT(*) FROM $DU_TBNAME ;" -o $TMPDIR/impalatest1.2.log &>> $LOG
	impala-shell -k -i $IMPALA_HOST -q "USE $DB_NAME; SELECT COUNT(*) FROM $AGG_TBNAME ;" -o $TMPDIR/impalatest1.3.log &>> $LOG
else
	impala-shell -i $IMPALA_HOST -q "USE $DB_NAME; SELECT COUNT(*) FROM $HIST_TBNAME ;" -o $TMPDIR/impalatest1.1.log &>> $LOG
	impala-shell -i $IMPALA_HOST -q "USE $DB_NAME; SELECT COUNT(*) FROM $DU_TBNAME ;" -o $TMPDIR/impalatest1.2.log &>> $LOG
	impala-shell -i $IMPALA_HOST -q "USE $DB_NAME; SELECT COUNT(*) FROM $AGG_TBNAME ;" -o $TMPDIR/impalatest1.3.log &>> $LOG
fi	
HIST=`grep -o -E '[0-9]*' $TMPDIR/impalatest1.1.log`
DUP=`grep -o -E '[0-9]*' $TMPDIR/impalatest1.2.log`
AGG=`grep -o -E '[0-9]*' $TMPDIR/impalatest1.3.log`

let TOT=$HIST+$DUP

if [ $TOT -eq $AGG ];
then
	echo "Test passed:" | tee -a $LOG
	echo "Number of records - $AGG ($AGG_TBNAME)" | tee -a $LOG
	echo "Number of records - $TOT ($HIST_TBNAME + $DU_TBNAME)" | tee -a $LOG
else
	echo "Test failed:" | tee -a $LOG
	echo "Number of records - $AGG ($AGG_TBNAME)" | tee -a $LOG
	echo "Number of records - $TOT ($HIST_TBNAME + $DU_TBNAME)" | tee -a $LOG
	echo "------------------------------------" | tee -a $LOG
	exit 1
fi	
echo "...done"
echo "------------------------------------" | tee -a $LOG

#############################################################
## Edit the daily update file with the correct information ##
#############################################################
echo "Editing daily_update.sh to reflect input information ..." | tee -a $LOG

cp skel/daily_update.skel.sh daily_update.sh

SAFEDIR=$(echo $LOGDIR | sed 's \([]\#\%\@\*$\/&[]\) \\\1 g')
sed -i "s/LOGDIR=/&$SAFEDIR/" daily_update.sh
sed -i "s/NR=/&$NRETRY/" daily_update.sh
sed -i "s/IMPALA_HOST=/&$IMPALA_HOST/" daily_update.sh
sed -i "s/KERBEROS=/&$KERBEROS/" daily_update.sh
sed -i "s/PARTITION=/&$PARTITION/" daily_update.sh
sed -i "s/DB_NAME=/&$DB_NAME/" daily_update.sh
sed -i "s/AGG_TBNAME=/&$AGG_TBNAME/" daily_update.sh
sed -i "s/DU_TBNAME=/&$DU_TBNAME/" daily_update.sh
SAFEURL=$(echo $DU_URL| sed 's \([]\#\%\@\*$\/&[]\) \\\1 g')
sed -i "s/URL=/&$SAFEURL/" daily_update.sh

SAFEDIR=$(echo $DU_TDIR | sed 's \([]\#\%\@\*$\/&[]\) \\\1 g')
sed -i "s/TDIR=/&$SAFEDIR/" daily_update.sh
SAFEDIR=$(echo $DU_DDIR | sed 's \([]\#\%\@\*$\/&[]\) \\\1 g')
sed -i "s/DDIR=/&$SAFEDIR/" daily_update.sh
SAFEDIR=$(echo $DU_HDFSDIR | sed 's \([]\#\%\@\*$\/&[]\) \\\1 g')
sed -i "s/HDFSDIR=/&$SAFEDIR/" daily_update.sh
echo "...done"
echo "------------------------------------" | tee -a $LOG

# Cleaning up temp files
echo "Cleaning up..." | tee -a $LOG
rm -rf $TMPDIR
echo "------------------------------------" | tee -a $LOG
exit 0
