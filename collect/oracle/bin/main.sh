#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/env.sh
. $CONF

exec &> >(tee -a "${DIRLOG}/main_0to1.log")

############################################################
############################################################
function get_from_remote() {
	#move remote files to local
	for fic in $(ssh $REMOTE_ORACLE_LOG_USER@$REMOTE_ORACLE_LOG_SERVER "cd $REMOTE_ORACLE_LOG_DIR_IN; ls *.zip 2>/dev/null")
	do
		echo "fic : $fic"
		scp -p $REMOTE_ORACLE_LOG_USER@$REMOTE_ORACLE_LOG_SERVER:$REMOTE_ORACLE_LOG_DIR_IN/$fic $LOCAL_ORACLE_LOG_DIR_IN &&\
		ssh $REMOTE_ORACLE_LOG_USER@$REMOTE_ORACLE_LOG_SERVER "mv $REMOTE_ORACLE_LOG_DIR_IN/$fic $REMOTE_ORACLE_LOG_DIR_DONE/$fic"
	done
}

############################################################
############################################################
function report_header() {
	echo "file analyzed;host;instance;date_min;date_max;source_filename;source_filedate;integration_date;file_first_date;file_last_date"
}

############################################################
function report_file() {
	filein=$1
	for server_instance in $(gunzip -c $filein | sed 1d | awk -F';' '{print $3 ";" $10}' | sort -u)
	do
		server=$(echo $server_instance | cut -d';' -f1)
		instance=$(echo $server_instance | cut -d';' -f2)
		source_filename=$(gunzip -c $filein | head -2 | tail -1 | cut -d';' -f11)
		source_filedate=$(gunzip -c $filein | head -2 | tail -1 | cut -d';' -f12)
		integration_date=$(gunzip -c $filein | head -2 | tail -1 | cut -d';' -f13)
		file_first_date=$(gunzip -c $filein | head -2 | tail -1 | cut -d';' -f14)
		file_last_date=$(gunzip -c $filein | head -2 | tail -1 | cut -d';' -f15)
		tmpfile=$(mktemp)
		gunzip -c $filein | awk -F';' -v server=$server -v instance=$instance '{if ($10 == instance && $3 == server) {print $1}}' | sed -e "s/^<txt>//g" | awk -F'-' '
                $2 ~ /JAN|01/ { print $3"01"$1}
                $2 ~ /FEB|02/ { print $3"02"$1}
                $2 ~ /MAR|03/ { print $3"03"$1}
                $2 ~ /APR|04/ { print $3"04"$1}
                $2 ~ /MAY|05/ { print $3"05"$1}
                $2 ~ /JUN|06/ { print $3"06"$1}
                $2 ~ /JUL|07/ { print $3"07"$1}
                $2 ~ /AUG|08/ { print $3"08"$1}
                $2 ~ /SEP|09/ { print $3"09"$1}
                $2 ~ /OCT|10/ { print $3"10"$1}
                $2 ~ /NOV|11/ { print $3"11"$1}
                $2 ~ /DEC|12/ { print $3"12"$1}
                '  | sort -n > $tmpfile
		date_min=$(head -1 $tmpfile)
		date_max=$(tail -1 $tmpfile)
		rm -f $tmpfile
		echo "$(basename $filein);$server;$instance;$date_min;$date_max;$source_filename;$source_filedate;$integration_date;$file_first_date;$file_last_date"
	done
}

############################################################
function parse_in() {

	#parse files to csv
	CURDIR=$PWD
	cd $LOCAL_ORACLE_LOG_DIR_IN
	CURDATE=$(date +"%Y%m%d")
	CURTIME=$(date +"%H%M%S")

	report_filename=$LOCAL_ORACLE_LOG_DIR_REPORT/report_${CURDATE}-${CURTIME}.csv
	report_error_filename=$LOCAL_ORACLE_LOG_DIR_REPORT/report_error_${CURDATE}-${CURTIME}.log
	report_header > ${report_filename}

	for fic in $(ls *.zip 2>/dev/null)
	do
		echo "fic : $fic"
		hostname=$(echo $fic | cut -d_ -f2)
		ficnoext=$(basename $fic .zip)_${CURDATE}.log
		filedate=$(ls -l --time-style long-iso $fic | awk '{print $6}')
		tmpfl=$(mktemp)
		PERLCMD="s/^.*(\d\d)-([^ |\.]+).*-(\d{2,4})\s(\d\d:\d\d:\d\d).*$/\1-\2-\3/"
		unzip -c $fic | perl -ne "print if $PERLCMD" > $tmpfl 
		firstDate=$(head -1  $tmpfl)
		lastdate=$(tail -1  $tmpfl)
		rm $tmpfl

		PERLCOMMAND1="s/([^ ]+)\s(\d\d:\d\d:\d\d)\s[^ ]+\s\(CONNECT_DATA=.*(SID|SERVICE_NAME)=([^\)]*)\).*PROGRAM=([^\)]*)\).*HOST=([^\)]*)\).*USER=([^\)]*)\).*\s.*HOST=([^\)]*)\).*\(PORT=(\d*).*establish\s[^ ]+\s([^ ]+)\s.*/\1;\2;\L${hostname};\4;\5;\6;\8;\7;\9;\$10\E;${fic};${filedate};${CURDATE};${firstDate};${lastdate}/"
		PERLCOMMAND2="s/([^ ]+)\s(\d\d:\d\d:\d\d)\s[^ ]+\s\(CONNECT_DATA=.*PROGRAM=([^\)]*)\).*HOST=([^\)]+)\).*USER=([^\)]*)\).*(SID|SERVICE_NAME|sid)=([^\)]+)\).*\s.*HOST=([^\)]*)\).*\(PORT=(\d+).*establish\s[^ ]+\s([^ ]+)\s.*/\1;\2;\L${hostname};\7;\3;\4;\8;\5;\9;\$10\E;${fic};${filedate};${CURDATE};${firstDate};${lastdate}/"
		PERLCOMMAND3="s/([^ ]+)\s(\d\d:\d\d:\d\d)\s[^ ]+\s\(CONNECT_DATA=.*PROGRAM=([^\)]*)\).*HOST=([^\)]+)\).*USER=([^\)]*)\).*(SID|SERVICE_NAME|sid)=([^\)]+)\).*\s.*establish\s[^ ]+\s([^ ]+)\s.*/\1;\2;\L${hostname};\7;\3;\4;\8;\5;unknown;unknown\E;${fic};${filedate};${CURDATE};${firstDate};${lastdate}/"
		PERLCOMMAND4="s/([^ ]+)\s(\d\d:\d\d:\d\d)\s[^ ]+\s\(CONNECT_DATA=.*(SID|SERVICE_NAME)=([^\)]*)\).*PROGRAM=([^\)]*)\).*HOST=([^\)]*)\).*USER=([^\)]*)\).*\s.*establish\s[^ ]+\s([^ ]+)\s.*/\1;\2;\L${hostname};\4;\5;\6;\8;\7;unknown;unknown\E;${fic};${filedate};${CURDATE};${firstDate};${lastdate}/"
		echo "date;time;host_name;host_sid;source_program;source_host_name;source_host_ip;source_user;host_port;host_instance;source_filename;source_filedate;integration_date;file_first_date;file_last_date" >  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext}
		unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if $PERLCOMMAND1" | sed -e "s/<txt>//g" | sed -e "s/^ //" >> $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} &&\
		unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if not $PERLCOMMAND1" | perl -ne "print if $PERLCOMMAND2" | sed -e "s/<txt>//g" | sed -e "s/^ //" >>  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} &&\
		unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if not $PERLCOMMAND1" | perl -ne "print if not $PERLCOMMAND2" | perl -ne "print if $PERLCOMMAND3" | sed -e "s/<txt>//g" | sed -e "s/^ //" >>  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} &&\
		unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if not $PERLCOMMAND1" | perl -ne "print if not $PERLCOMMAND2" | perl -ne "print if not $PERLCOMMAND3" | perl -ne "print if $PERLCOMMAND4" | sed -e "s/<txt>//g" | sed -e "s/^ //" >>  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} &&\
		gzip  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} && chmod 666 $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext}.gz &&\
		unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if not $PERLCOMMAND1" | perl -ne "print if not $PERLCOMMAND2" > $LOCAL_ORACLE_LOG_DIR_TRASH/${ficnoext} &&\
		mv $fic $LOCAL_ORACLE_LOG_DIR_DONE &&\
		echo "reporting..." &&\
		report_file $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext}.gz >> ${report_filename}
		ret=$?
		[[ $ret -ne 0 ]] && echo "Error in processing file $fic" | tee -a ${report_error_filename}
	done
	cd $CURDIR
	aws s3 cp ${report_filename} s3://gecustomers/document/GPI/oracle_log/

}

############################################################
#main
get_from_remote
parse_in

