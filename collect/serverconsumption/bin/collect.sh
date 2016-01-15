#!/usr/bin/env bash

SERVERLST=$(dirname $0)/../conf/server.lst
URL="http://iww.dcs.itssc.alstom.com/nrtmd/streamsdump/server"

if [[ $# -ne 0 ]]
then
	SERVERLST=$1
fi

suffix="$(date +"%Y%m%d-%H%M%S")"
dirout="/datalab3/DATA/SERVER"

echo "$(date +"%Y/%m/%d-%H:%M:%S") : $0 : start"

for SERVER in $(cat $SERVERLST |awk '{print $1}')
do
	CMD="curl $URL/$SERVER  --compressed"
	echo "$(date +"%Y/%m/%d-%H:%M:%S") : $0 : CMD : $CMD > $dirout/${SERVER}_$suffix.xls"
	$CMD > $dirout/${SERVER}_$suffix.xls
done
echo "$(date +"%Y/%m/%d-%H:%M:%S") : $0 : end"
