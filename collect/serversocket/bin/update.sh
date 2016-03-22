#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

DATECUR=$(date --utc --date "now" +"%Y%m%d-%H%M%S")

cp -f $SERVERSTATUS $SERVERSTATUS.$DATECUR
for host in $(cat $SERVERSTATUS | awk -F';' '{if ($3 == "2") print $1}')
do 
	status=2
	ip=$(grep -i "^$host;" $SERVERLIST | awk -F';' '{print $5}')
	ostype=$(grep -i "^$host;" $SERVERLIST | awk -F';' '{print $10}')
	[[ "$ip" == "" ]]|| [[ "$ip" == "" ]] && echo "$host not found in  $SERVERLIST. continue" && continue
	$ROOTDIR/bin/submit.sh update $ip $host $ostype && status=1
	echo "$0 : $host;$ip;$status;$DATECUR"
done

