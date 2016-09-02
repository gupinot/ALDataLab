#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/env.sh
. $CONF

#move remote files to local
for fic in $(ssh $REMOTE_ORACLE_LOG_USER@$REMOTE_ORACLE_LOG_SERVER "cd $REMOTE_ORACLE_LOG_DIR_IN; ls")
do
	echo "fic : $fic"
	scp $REMOTE_ORACLE_LOG_USER@$REMOTE_ORACLE_LOG_SERVER:$REMOTE_ORACLE_LOG_DIR_IN/$fic $LOCAL_ORACLE_LOG_DIR_IN &&\
	ssh $REMOTE_ORACLE_LOG_USER@$REMOTE_ORACLE_LOG_SERVER "mv $REMOTE_ORACLE_LOG_DIR_IN/$fic $REMOTE_ORACLE_LOG_DIR_DONE/$fic"
done

#parse files to csv
CURDIR=$PWD
cd $LOCAL_ORACLE_LOG_DIR_IN


for fic in $(ls *.zip)
do
	echo "fic : $fic"
	hostname=$(echo $fic | cut -d_ -f2)
	ficnoext=$(basename $fic .zip).log
	PERLCOMMAND1="s/([^ ]+)\s(\d\d:\d\d:\d\d)\s[^ ]+\s\(CONNECT_DATA=.*(SID|SERVICE_NAME)=([^\)]*)\).*PROGRAM=([^\)]*)\).*HOST=([^\)]*)\).*USER=([^\)]*)\).*\s.*HOST=([^\)]*)\).*\(PORT=(\d*).*establish\s[^ ]+\s([^ ]+)\s.*/\1;\2;${hostname};\4;\5;\6;\8;\7;\9;\$10/"
	PERLCOMMAND2="s/([^ ]+)\s(\d\d:\d\d:\d\d)\s[^ ]+\s\(CONNECT_DATA=.*PROGRAM=([^\)]*)\).*HOST=([^\)]+)\).*USER=([^\)]*)\).*(SID|SERVICE_NAME|sid)=([^\)]+)\).*\s.*HOST=([^\)]*)\).*\(PORT=(\d+).*establish\s[^ ]+\s([^ ]+)\s.*/\1;\2;${hostname};\7;\3;\4;\8;\5;\9;\$10/"
	echo "date;time;host_name;host_sid;source_program;source_host_name;source_host_ip;source_user;host_port;host_instance" >  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext}
	unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if $PERLCOMMAND1" >> $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} &&\
	unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if not $PERLCOMMAND1" | perl -ne "print if $PERLCOMMAND2" >>  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} &&\
	gzip  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} && chmod 666 $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext}.gz
	unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if not $PERLCOMMAND1" | perl -ne "print if not $PERLCOMMAND2" > $LOCAL_ORACLE_LOG_DIR_TRASH/${ficnoext} &&\
	mv $fic $LOCAL_ORACLE_LOG_DIR_DONE 
done

cd $CURDIR
