#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

exec &> >(tee -a "${LOG}/collect.log")

if [[ $# -ne 0 ]]
then
	SERVERLST=$1
fi

suffix="$(date +"%Y%m%d-%H%M%S")"

echo "$(date +"%Y/%m/%d-%H:%M:%S") : $0 : start"

for SERVER in $(cat $SERVERLST |awk '{print $1}')
do
	CMD="curl $URL/$SERVER  --compressed"
	echo "$(date +"%Y/%m/%d-%H:%M:%S") : $0 : CMD : $CMD > $dirserverxls/${SERVER}_$suffix.xls"
	$CMD > $dirserverxls/${SERVER}_$suffix.xls
done
echo "$(date +"%Y/%m/%d-%H:%M:%S") : $0 : execute convert-send script..."
$ROOTDIR/bin/convert-send.sh
echo "$(date +"%Y/%m/%d-%H:%M:%S") : $0 : end"
