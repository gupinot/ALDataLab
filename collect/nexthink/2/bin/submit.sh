#!/usr/bin/env bash

DirLog=$HOME/log; 
if [[ ! -d "${DirLog}" ]] 
then
	mkdir "${DirLog}"
fi
exec &> >(tee -a "${DirLog}/submit.log")

WD=$(dirname $0)
if [[ "$WD" == "." ]]
then
	WD=$(pwd)
fi

. $WD/../conf/conf.sh

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : begin"
########################################################
#Parse repo in files, validate and send to s3 (expect I-ID)
#${REPO_SHELL}

########################################################
#Parse Nexthink csv files (connection and webrequest) list and :
# - anonymize I_U_ID and I_D_ID
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Parse nexthink connection files"
${DISTRIB_SHELL} "0" "${INNXFILES}/connection*.gz" "${NXPIPE_RSHELL}" "connection"
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Parse nexthink webrequest files"
${DISTRIB_SHELL} "0" "${INNXFILES}/webrequest*.gz" "${NXPIPE_RSHELL}" "webrequest"

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Parse nexthink execution files"
${DISTRIB_SHELL} "0" "${INNXFILES}/execution*.gz" "${NXPIPE_RSHELL}" "execution"

# - merge anonymized files by file date and split if result too large
${MERGE_SPLIT_SHELL}

# - send to s3
${SENDMERGEDSPLITED_SHELL}
# also executed in crontab

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Parse oracle files"
${DISTRIB_SHELL} "0" "${INORAFILES}/listener*.gz" "${ORAPIPE_RSHELL}" "listener"
${DISTRIB_SHELL} "0" "${INORAFILES}/LISTENER*.gz" "${ORAPIPE_RSHELL}" "listener"
${SENDORACLE_SHELL}

########################################################
#Create updated version of I-ID csv file and send to s3
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Anonymize IDM"
${ANONYMIZEIDM_SHELL}


echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : End"

