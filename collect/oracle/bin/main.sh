#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/env.sh
. $CONF

exec &> >(tee -a "${DIRLOG}/main_0to1.log")

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
function report_header() {
	echo "file analyzed;host;instance;date_min;date_max;source_filename;source_filedate;integration_date"
}
function report_file() {
	filein=$1
	for server_instance in $(gunzip -c $filein | sed 1d | awk -F';' '{print $3 ";" $10}' | sort -u)
	do
		server=$(echo $server_instance | cut -d';' -f1)
		instance=$(echo $server_instance | cut -d';' -f2)
		source_filename=$(gunzip -c $filein | head -2 | tail -1 | cut -d';' -f11)
		source_filedate=$(gunzip -c $filein | head -2 | tail -1 | cut -d';' -f12)
		integration_date=$(gunzip -c $filein | head -2 | tail -1 | cut -d';' -f13)
		tmpfile=$(mktemp)
		gunzip -c $filein | awk -F';' -v server=$server -v instance=$instance '{if ($10 == instance && $3 == server) {print $1}}' | sed -e "s/^<txt>//g" | awk -F'-' '
                $2 ~ /JAN/ { print $3"01"$1}
                $2 ~ /FEB/ { print $3"02"$1}
                $2 ~ /MAR/ { print $3"03"$1}
                $2 ~ /APR/ { print $3"04"$1}
                $2 ~ /MAY/ { print $3"05"$1}
                $2 ~ /JUN/ { print $3"06"$1}
                $2 ~ /JUL/ { print $3"07"$1}
                $2 ~ /AUG/ { print $3"08"$1}
                $2 ~ /SEP/ { print $3"09"$1}
                $2 ~ /OCT/ { print $3"10"$1}
                $2 ~ /NOV/ { print $3"11"$1}
                $2 ~ /DEC/ { print $3"12"$1}
                '  | sort -n > $tmpfile
		date_min=$(head -1 $tmpfile)
		date_max=$(tail -1 $tmpfile)
		rm -f $tmpfile
		echo "$(basename $filein);$server;$instance;$date_min;$date_max;$source_filename;$source_filedate;$integration_date"
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

		PERLCOMMAND1="s/([^ ]+)\s(\d\d:\d\d:\d\d)\s[^ ]+\s\(CONNECT_DATA=.*(SID|SERVICE_NAME)=([^\)]*)\).*PROGRAM=([^\)]*)\).*HOST=([^\)]*)\).*USER=([^\)]*)\).*\s.*HOST=([^\)]*)\).*\(PORT=(\d*).*establish\s[^ ]+\s([^ ]+)\s.*/\1;\2;\L${hostname};\4;\5;\6;\8;\7;\9;\$10\E;${fic};${filedate};${CURDATE}/"
		PERLCOMMAND2="s/([^ ]+)\s(\d\d:\d\d:\d\d)\s[^ ]+\s\(CONNECT_DATA=.*PROGRAM=([^\)]*)\).*HOST=([^\)]+)\).*USER=([^\)]*)\).*(SID|SERVICE_NAME|sid)=([^\)]+)\).*\s.*HOST=([^\)]*)\).*\(PORT=(\d+).*establish\s[^ ]+\s([^ ]+)\s.*/\1;\2;\L${hostname};\7;\3;\4;\8;\5;\9;\$10\E;${fic};${filedate};${CURDATE}/"
		echo "date;time;host_name;host_sid;source_program;source_host_name;source_host_ip;source_user;host_port;host_instance;source_filename;source_filedate;integration_date" >  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext}
		unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if $PERLCOMMAND1" | sed -e "s/<txt>//g" | sed -e "s/^ //" >> $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} &&\
		unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if not $PERLCOMMAND1" | perl -ne "print if $PERLCOMMAND2" | sed -e "s/<txt>//g" | sed -e "s/^ //" >>  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} &&\
		gzip  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} && chmod 666 $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext}.gz &&\
		unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if not $PERLCOMMAND1" | perl -ne "print if not $PERLCOMMAND2" > $LOCAL_ORACLE_LOG_DIR_TRASH/${ficnoext} &&\
		mv $fic $LOCAL_ORACLE_LOG_DIR_DONE &&\
		echo "reporting..." &&\
		report_file $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext}.gz >> ${report_filename}
		ret=$?
		[[ $ret -ne 0 ]] && echo "Error in processing file $fic" | tee -a ${report_error_filename}
	done
	cd $CURDIR
}


############################################################
#main
get_from_remote
parse_in

