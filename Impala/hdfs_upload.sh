#!/bin/bash

# Name: hdfs_upload.sh
# Author: Pierre-Yves Taunay (py.taunay@psu.edu)
# Description:
# This script uploads downloaded files from the specified local directory
# to the specified HDFS directory

# Return values:
# 0: Success
# 1: Unspecified failure
# 2: Bad number of arguments

####################
## Usage function ##
####################
usage() {
	echo "hdfs_upload usage"
	echo "Required arguments:"
	echo " --localdir	: local directory of the files to upload"
	echo " --hdfsdir	: HDFS destination directory of the files"
	echo " --log		: location of the log file to use" 
	echo " Optional arguments "
	echo " --verbose | -v 	: verbose mode"
	echo " --help | -h 	: print this help message"
}	

##########################
## Parse the input line ##
##########################
TMP=$(getopt -o l:f:vh --long localdir:,hdfsdir:,log:,verbose,help -n '$0' -- "$@")

if [ $? -ne 0 ];
then
	echo "ERROR Problem with getopt, exiting now." | tee -a $LOG
	exit 1
fi	

eval set -- "$TMP"

if [[ $# -lt 2 || $# -gt 9 ]];
then
	echo "ERROR Wrong number of arguments in hdfs_upload" | tee -a $LOG
	usage
	exit 2
fi	

## Variables
LDIR=
HDFSDIR=
VERBOSE=0
LOG=

while true;
do
	case "$1" in
		-h|--help)
		usage
		exit 0
		shift ;;
		-v|--verbose)
			VERBOSE=1
			shift ;;
		--localdir)
			LDIR=$2
			shift 2;;
		--hdfsdir)
			HDFSDIR=$2
			shift 2;;
		--log)
			LOG=$2
			shift 2;;
		--)
			shift
			break;;
	esac
done	

## Output data if verbose
if [ $VERBOSE -eq 1 ];
then
	echo "HDFS_USAGE" | tee -a $LOG
	echo "INFO Options chosen" | tee -a $LOG
	echo "INFO Local directory to upload: $LDIR" |  tee -a $LOG
	echo "INFO HDFS target directory: $HDFSDIR" | tee -a $LOG
	echo "INFO Verbose mode: $VERBOSE" | tee -a $LOG
fi	

## Upload TSV files on HDFS
# Test if the directory exists
hadoop fs -test -e $HDFSDIR
OUT=$?
echo "Uploading files from local ($LDIR) to HDFS ($HDFSDIR) ..." | tee -a $LOG
if ! [[ $OUT -eq 0 ]];
then
	# If it does not, attempt at creating it and deal with error
	echo "$HDFSDIR does not exist; creating..." | tee -a $LOG
	hadoop fs -mkdir -p $HDFSDIR &>> $LOG
	OUT=$?
	if ! [[ $OUT -eq 0 ]];
	then
		echo "ERROR Could not create $HDFSDIR (Error $OUT -- Wrong permissions ? Wrong path ?)" | tee -a $LOG
		exit 1
	fi	
	echo "...done" | tee -a $LOG
fi	
# Move files to HDFS
hadoop fs -copyFromLocal $LDIR/* $HDFSDIR/ &>> $LOG
echo "...done" | tee -a $LOG

exit 0
