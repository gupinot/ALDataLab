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

PERLCOMMAND1=" s/([^ ]+)\s(\d\d:\d\d:\d\d)\s[^ ]+\s\(CONNECT_DATA=.*(SID|SERVICE_NAME)=([^\)]*)\).*PROGRAM=([^\)]*)\).*HOST=([^\)]*)\).*USER=([^\)]*)\).*\s.*PORT=(\d*).*establish\s[^ ]+\s([^ ]+)\s.*/\1;\2;\4;\5;\6;\7;\8;\9/"
PERLCOMMAND2="s/([^ ]+)\s(\d\d:\d\d:\d\d)\s[^ ]+\s\(CONNECT_DATA=.*PROGRAM=([^\)]*)\).*HOST=([^\)]+)\).*USER=([^\)]*)\).*(SID|SERVICE_NAME|sid)=([^\)]+)\).*\s.*PORT=(\d+).*establish\s[^ ]+\s([^ ]+)\s.*/\1;\2;\7;\3;\4;\5;\8;\9/"

for fic in $(ls *.zip)
do
	echo "fic : $fic"
	ficnoext=$(basename $fic .zip).log
	echo "date;time;sid;program;host;user;port;instance" >  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext}
	unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if $PERLCOMMAND1" >> $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} &&\
	unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if not $PERLCOMMAND1" | perl -ne "print if $PERLCOMMAND2" >>  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext} &&\
	gzip  $LOCAL_ORACLE_LOG_DIR_ENCODED/${ficnoext}
	unzip -c $fic | grep CONNECT_DATA | grep establish | perl -ne "print if not $PERLCOMMAND1" | perl -ne "print if not $PERLCOMMAND2" > $LOCAL_ORACLE_LOG_DIR_TRASH/${ficnoext} &&\
	mv $fic $LOCAL_ORACLE_LOG_DIR_DONE 
done


