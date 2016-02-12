#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

DATECUR=$(date +"%Y%m%d-%H%M%S")

for host in $(cat $SERVERSTATUS | awk -F';' '{if ($3 == "2") print $1}')
do 
	status=1
	ip=$(grep -i "^$host;" $SERVERLIST | awk -F';' '{print $5}')
	$ROOTDIR/bin/submit.sh collect $ip && status=0
	tmpfile=$(mktemp)
	echo "$host;$ip;$status;$DATECUR" >> $SERVERCOLLECT
	echo "$0 : $host;$ip;$status;$DATECUR"
done



