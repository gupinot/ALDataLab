#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

DATECUR=$(date --utc --date "now" +"%Y%m%d-%H%M%S")

cp -f $SERVERSTATUS $SERVERSTATUS.$DATECUR
for host in $(cat $SERVERSTATUS | awk -F';' '{if ($3 == "1") print $1}')
do 
	status=1
	ip=$(grep -i "^$host;" $SERVERLIST | awk -F';' '{print $5}')
	$ROOTDIR/bin/submit.sh deploy $ip && status=2
	tmpfile=$(mktemp)
	grep -v "^$host;" $SERVERSTATUS > $tmpfile
	echo "$host;$ip;$status;$DATECUR" >> $tmpfile
	echo "$0 : $host;$ip;$status;$DATECUR"
	cp -f $tmpfile $SERVERSTATUS
done


