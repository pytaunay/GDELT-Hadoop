#!/bin/bash

# This script is a download engine for the GDELT database  
# Required arguments:
# --targetdir | -t	: target location to download the files
# --datadir | -d	: target location to extract the files
# --url	 | -u		: URL of the target website
# Optional arguments 
# --nretry | -r	: number of retries before dropping a download (default=5,limit=20)
# --nproc | -p		: number of processes to spawn when unzipping the files (default=1,limit=16)
# --verbose | -v
# --help | -h

# Return values
# 0: success
# 1: fatal error
# 2: wrong nubmer of arguments
# 3: datadir or targetdir do not have correct permissions 

####################
## Usage function ##
####################
usage() {
	echo "dl_engine usage"
	echo "Required arguments:"
	echo " --targetdir | -t	: target location to download the files"
	echo " --datadir | -d	: target location to extract the files"
	echo " --url	 | -u	: URL of the target website"
	echo " --log		: location of the log file"
	echo " Optional arguments "
	echo " --nretry | -r	: number of retries before dropping a download (default=5,limit=20)"
	echo " --nproc | -p		: number of processes to spawn when unzipping the files (default=1,limit=16)"
	echo " --verbose | -v : verbose mode"
	echo " --help | -h : print this help message"
}	

##########################
## Parse the input line ##
##########################
TMP=$(getopt -o t:d:u:r:p:vh --long log:,targetdir:,datadir:,url:,nretry:,nproc:,verbose,help -n '$0' -- "$@")

if [ $? -ne 0 ];
then
	echo "ERROR Problem with getopt, exiting now."
	exit 1
fi	

eval set -- "$TMP"

if [[ $# -lt 2 || $# -gt 15 ]];
then
	echo "ERROR Wrong number of arguments"
	usage
	exit 2
fi	


URL=
TDIR=
DDIR=
NP=1
NR=5
VERBOSE=0

while true; 
do
#	echo "$1"
	case "$1" in
		-h|--help)
		usage
		exit 0
		shift ;;	
		-v|--verbose)
			VERBOSE=1
			shift ;;
		-t|--targetdir)
			TDIR=$2
			shift 2;;
		-d|--datadir)
			DDIR=$2
			shift 2;;
		-u|--url)
			URL=$2
			shift 2;;
		-r|--nretry)
			# Check if NR is a number between 1 and 20 
			INT='^[0-9]*$'
			if ! [[ $2 =~ $INT ]];
			then
				echo "ERROR --nretry/-nr: $2 is not a number"
				exit 2
			else
				if ! [[ $2 -ge 1 && $2 -le 20 ]];
				then
					echo "ERROR --nretry/-nr: $2 is not within boundaries (1,20)"
					exit 2
				else
					NR=$2
				fi
			fi	
			shift 2;;
		-p|--nproc)
			# Check if NP is a number between 1 and 16 
			INT='^[0-9]*$'
			if ! [[ $2 =~ $INT ]];
			then
				echo "ERROR --nproc/-np: $2 is not a number"
				exit 2
			else
				if ! [[ $2 -ge 1 && $2 -le 16 ]];
				then
					echo "ERROR --nretry/-nr: $2 is not within boundaries (1,16)"
					exit 2
				else
					NP=$2
				fi
			fi	
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
	echo "DL ENGINE" | tee -a $LOG
	echo "INFO Options chosen" | tee -a $LOG
	echo "INFO Target directory for zip files: $TDIR" | tee -a $LOG
	echo "INFO Target directory for data files: $DDIR" | tee -a $LOG
	echo "INFO Target URL: $URL" | tee -a $LOG
	echo "INFO Number of processes for unzip: $NP" | tee -a $LOG
	echo "INFO Number of retries: $NR" | tee -a $LOG
	echo "INFO Verbose mode: $VERBOSE" | tee -a $LOG
fi

##########################################
## Check if specified directories exist ##
##########################################
if [ ! -d "$TDIR" ]; 
then
	mkdir -p $TDIR &> /dev/null
	OUT=$?
	if ! [[ $OUT -eq 0 ]];
	then
		echo "ERROR Could not create $TDIR (Error $OUT -- Wrong permissions ?)" | tee -a $LOG
		exit 1;
	fi	
fi

if [ ! -d "$DDIR" ]; 
then
	mkdir -p $DDIR &> /dev/null
	OUT=$?
	if ! [[ $? -eq 0 ]];
	then
		echo "ERROR Could not create $DDIR (Error $OUT -- Wrong permissions ?)" | tee -a $LOG
		exit 1;
	fi	
fi

########################
## Download the files ##
########################
echo "Downloading the files at $URL..." | tee -a $LOG
wget -r -nd -nc --no-parent --reject="index.html*" -P $TDIR -e robots=off $URL -a $LOG
echo "...done" | tee -a $LOG

#######################################################
## Test that the zip files were downloaded correctly ##
#######################################################
echo "Checking integrity of the compressed files, patience..." | tee -a $LOG
for file in `ls $TDIR`; do
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

#####################
## Unzip the files ##		
#####################
echo "Unzipping the compressed files..." | tee -a $LOG
# Replace any unsafe (i.e. special) characters to be able to parse with sed
SAFEDIR=$(echo $TDIR | sed 's \([]\#\%\@\*$\/&[]\) \\\1 g')
# Unzip the files w/ 8 processes in parallel
ls $TDIR | awk 'NR > 1 {print $1}' |  sed "s/^/$SAFEDIR\//g"| xargs -n 1 -P $NP unzip -q -d $DDIR/
echo "...done" | tee -a $LOG

exit 0

