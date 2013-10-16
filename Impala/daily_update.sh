#!/bin/bash

# Name: daily_update.sh
# Author: Pierre-Yves Taunay (py [dot] taunay [at] psu [dot] edu)
# Description: this script compares the last update date in the GDELT Impala database to yesterday's date.
# If the dates differ, then the database is outdated. Necessary files are then downloaded and added to the database. 

EXT=`date +%Y%m%d.%H%M%S`

## Log directory
LOGDIR=
LOG=daily_update.log.$EXT
LOG=$LOGDIR/$LOG

## Number of retries before skipping a file download
NR=

## Location of the Impala daemon
IMPALA_HOST=
## Kerberos authentication
KERBEROS=
## Partitioning
PARTITION=

## Database name, and aggregate table name
DB_NAME=
AGG_TBNAME=
DU_TBNAME=

## Daily update URL and directories
URL=
TDIR=
DDIR=
HDFSDIR=

##########################################################
## 		 SCRIPT STARTS HERE 			##
##########################################################
# Get the max. date currently in the daily update database
TMPDIR=/tmp/dailyupdate.$EXT
if [ ! -d $TMPDIR ];
then
	mkdir -p $TMPDIR &> /dev/null
fi	

if [ $KERBEROS -eq 1 ];
then
	impala-shell -k -i $IMPALA_HOST -o $TMPDIR/max.log -q "USE $DB_NAME; SELECT MAX(dateadded) FROM $AGG_TBNAME;" | tee -a $LOG
else	
	impala-shell -i $IMPALA_HOST -o $TMPDIR/max.log -q "USE $DB_NAME; SELECT MAX(dateadded) FROM $AGG_TBNAME;" | tee -a $LOG
fi	

# The data files uploaded on a given day correspond to a "dateadded" of the day before
# Therefore, have to compare the current database date DB_DATE to yesterday's date YTD_DATE
DB_DATE=$(sed -n '/[0-9]/p' $TMPDIR/max.log | awk '{print $2}')  
YTD_DATE=`date --date yesterday "+%Y%m%d"`

echo "-----------------------------------" | tee -a $LOG
if [[ $YTD_DATE -le $DB_DATE ]];
then
	echo "The database is currently up to date (yesterday's date: $YTD_DATE; database's date: $DB_DATE)" | tee -a $LOG
	exit 0
else
	echo "The database is out-dated (yesterday's date: $YTD_DATE; database's date: $DB_DATE)" | tee -a $LOG
	echo "Retrieving file list ..." | tee -a $LOG
fi	

# Retrieve the list of files currently uploaded
wget --no-remove-listing -N $URL -o $LOG 

if ! [[ -f "index.html" ]];
then
	echo "ERROR Could not retrieve the remote file list. Aborting ..." | tee -a $LOG
	exit 1
fi

# Find the latest file uploaded (i.e. the max date -- worst case it is equal to yesterday's date)
grep -o -E "\"[a-Z0-9.]*\"" index.html | grep -o -E "[0-9]*" > $TMPDIR/complete_list
REMOTE_DATE=`cat $TMPDIR/complete_list |  sort -g | tail -n 1`

# Retrieve the list of files to download
awk -v dbdate="$DB_DATE" -v rmdate="$REMOTE_DATE" '{ if ($1 > dbdate && $1 <= rmdate ){print $1}}' $TMPDIR/complete_list | sed "s/$/\.export\.CSV\.zip/g" >> $TMPDIR/DL_list 

echo "-----------------------------------" | tee -a $LOG
echo "`cat $TMPDIR/DL_list | wc -l` files will be downloaded" | tee -a $LOG
echo "File list:" | tee -a $LOG
echo "`cat $TMPDIR/DL_list`" | tee -a $LOG
echo "Starting download ..." | tee -a $LOG
for file in `cat $TMPDIR/DL_list`
do
	wget -nd -nc --no-parent --reject="index.html*" -P $TDIR -e robots=off $URL/$file -a $LOG 
done
echo "...done"

# Very the zip files for all the files
echo "-----------------------------------" | tee -a $LOG
echo "Verifying the downloaded files..."
for file in `cat $TMPDIR/DL_list`
do
	CORRUPT=1
	TRY=1
	while [[ $CORRUPT -eq 1 && $TRY -lt $NR ]]; do
		let TRY=$TRY+1
		unzip -tq $TDIR/$file &> /dev/null
		if [ $? -eq 0 ]; then
			CORRUPT=0
			echo "INFO $file OK" | tee -a $LOG
		else
			CORRUPT=1
			echo "ERROR $file corrupted, re-downloading [ $TRY / $NR retries ]" | tee -a $LOG
			rm $TDIR/$file
			wget -r --no-parent --reject="index.html*" -nd -nc -e robots=off -P $TDIR $URL/$file -a $LOG 
		fi

		if [ $TRY -ge $NR ]; then
			echo "ERROR $file: too many retries, skipping..." | tee -a $LOG
			rm $TDIR/$file
		fi	
	done
done	
echo "...done" | tee -a $LOG

echo "-----------------------------------" | tee -a $LOG
echo "Unzipping the compressed files..." | tee -a $LOG
for file in `cat $TMPDIR/DL_list`
do
	unzip -q $TDIR/$file -d $DDIR/
done
echo "...done" | tee -a $LOG


echo "-----------------------------------" | tee -a $LOG
echo "Renaming files to .tsv files..." | tee -a $LOG
rename .CSV .tsv $DDIR/*
echo "...done" | tee -a $LOG

## Remove the .CSV.zip on the files to upload them to HDFS
cat $TMPDIR/DL_list | sed "s/.CSV.zip/.tsv/g" >> $TMPDIR/HDFS_list

echo "-----------------------------------" | tee -a $LOG
echo "Uploading files from local ($DDIR) to HDFS ($HDFSDIR) ..." | tee -a $LOG
## Uploading the files to HDFS
for file in `cat $TMPDIR/HDFS_list`
do
	hadoop fs -copyFromLocal $DDIR/$file $HDFSDIR/ &>> $LOG
done
echo "...done" | tee -a $LOG
	

## Insert records into the database 
echo "-----------------------------------" | tee -a $LOG
echo "Creating the daily update query ..." | tee -a $LOG

if [ $PARTITION -eq 0 ];
then
	# If there are no partitions, the insertion is actually very easy. Select all fields.
	echo "USE $DB_NAME;" > daily_insert.sql
	# Dont forget to refresh to reflect changes in HDFS
	echo "REFRESH $DU_TBNAME;" >> daily_insert.sql 
	echo "INSERT INTO $AGG_TBNAME SELECT * FROM $DU_TBNAME WHERE dateadded > $DB_DATE AND dateadded <= $YTD_DATE;" >> daily_insert.sql 

else
	# If a partition exists, have to pay attention to how fields are organized
	cp skel/daily.skel.sql daily_insert.sql
	sed -i "s/USE/USE $DB_NAME/" daily_insert.sql
	sed -i "s/REFRESH/REFRESH $DU_TBNAME/" daily_insert.sql
	sed -i "s/INSERT INTO/INSERT INTO $AGG_TBNAME/" daily_insert.sql 
	sed -i "s/FROM/FROM $DU_TBNAME/" daily_insert.sql
	echo "WHERE dateadded > $DB_DATE AND dateadded <= $YTD_DATE;" >> daily_insert.sql
fi	
echo "...done" | tee -a $LOG

echo "-----------------------------------" | tee -a $LOG
echo "Insert records in the aggregate database ..." | tee -a $LOG

if [ $KERBEROS -eq 1 ];
then
	impala-shell -k -i $IMPALA_HOST -f daily_insert.sql &>> $LOG
else	
	impala-shell -i $IMPALA_HOST -f daily_insert.sql &>> $LOG
fi	

echo "...done" | tee -a $LOG

echo "------------------------------------" | tee -a $LOG
echo "Cleaning up..." | tee -a $LOG
rm -rf $TMPDIR	
echo "------------------------------------" | tee -a $LOG

exit 0
