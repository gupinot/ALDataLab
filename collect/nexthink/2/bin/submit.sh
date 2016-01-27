#!/usr/bin/env bash

WD=$(dirname $0)
if [[ "$WD" == "." ]]
then
	WD=$(pwd)
fi

. $WD/../conf/conf.sh

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : begin"
########################################################
#Parse repo in files, validate and send to s3 (expect I-ID)
${REPO_SHELL}

########################################################
#Parse Nexthink csv files (connection and webrequest) list and :
# - anonymize I_U_ID and I_D_ID
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Parse nexthink connection files"
${DISTRIB_SHELL} "0 1 2 3" "${INNXFILES}/connection*.gz" "${NXPIPE_RSHELL}" "connection"
${DISTRIB_SHELL} "0 1 2 3" "${INNXFILES}/connection*.gz" "${NXPIPE_RSHELL}" "connection"
${DISTRIB_SHELL} "0 1 2 3" "${INNXFILES}/connection*.gz" "${NXPIPE_RSHELL}" "connection"
${DISTRIB_SHELL} "0 1 2 3" "${INNXFILES}/connection*.gz" "${NXPIPE_RSHELL}" "connection"
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Parse nexthink webrequest files"
${DISTRIB_SHELL} "0 1 2 3" "${INNXFILES}/webrequest*.gz" "${NXPIPE_RSHELL}" "webrequest"
${DISTRIB_SHELL} "0 1 2 3" "${INNXFILES}/webrequest*.gz" "${NXPIPE_RSHELL}" "webrequest"
${DISTRIB_SHELL} "0 1 2 3" "${INNXFILES}/webrequest*.gz" "${NXPIPE_RSHELL}" "webrequest"
${DISTRIB_SHELL} "0 1 2 3" "${INNXFILES}/webrequest*.gz" "${NXPIPE_RSHELL}" "webrequest"

# - merge anonymized files by file date and split if result too large
${MERGE_SPLIT_SHELL}

# - send to s3
#${SENDMERGEDSPLITED_SHELL}
# also executed in crontab

########################################################
#Create updated version of I-ID csv file and send to s3
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Anonymize IDM"
${ANONYMIZEIDM_SHELL}


echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : End"

