#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

CSC_IN=$ROOTDIR/conf/datalab_csc_done.csv
DATECUR=$(date --utc --date "now" +"%Y%m%d-%H%M%S")

cp -f $SERVERSTATUS $SERVERSTATUS.$DATECUR
for host in $(cat $CSC_IN | awk -F';' '{if ($3 = "done") print $1}')
do 
	status=0
	ip=$(grep -i "^$host;" $SERVERLIST | awk -F';' '{print $5}')
	serverstatus=$(cat $SERVERSTATUS | grep "^$host;")
	([[ "$serverstatus" == "" ]] || [[ $(echo $serverstatus | awk -F';' '{print $3}') -eq 0 ]]) &&\
	($ROOTDIR/bin/submit.sh test $ip $host && status=1
	 tmpfile=$(mktemp)
	 grep -v "^$host;" $SERVERSTATUS > $tmpfile
	 echo "$host;$ip;$status;$DATECUR" >> $tmpfile
	 cp -f $tmpfile $SERVERSTATUS
	)
done

