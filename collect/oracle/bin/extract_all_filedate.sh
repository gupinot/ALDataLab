#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/env.sh
. $CONF

exec &> >(tee -a "${DIRLOG}/report_filedate.log")

############################################################
function report_done() {

	#parse files to csv
	CURDIR=$PWD
	cd $LOCAL_ORACLE_LOG_DIR_DONE
	CURDATE=$(date +"%Y%m%d")
	CURTIME=$(date +"%H%M%S")

	report_filename=$LOCAL_ORACLE_LOG_DIR_REPORT/report_filedate_${CURDATE}-${CURTIME}.csv
	echo "source_filename;file_first_date;file_last_date" > ${report_filename}

	for fic in $(ls *.zip 2>/dev/null)
	do
		echo "fic : $fic"
		tmpfl=$(mktemp)
		PERLCMD="s/^ *(\d\d-...-\d{2,4}).*$/\1/"
		unzip -c $fic | perl -ne "print if $PERLCMD" > $tmpfl 
		firstDate=$(head -1  $tmpfl)
		lastdate=$(tail -1  $tmpfl)
		rm $tmpfl
		echo "$fic;$firstDate;$lastdate" >> ${report_filename}
		echo "$fic;$firstDate;$lastdate"
	done
	cd $CURDIR
	aws s3 cp ${report_filename} s3://gecustomers/document/GPI/oracle_log/

}

############################################################
#main
report_done

