#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF


DATECUR=$(date --utc --date "now" +"%Y-%m-%d_%H%M%S")

aws s3 cp ${SERVERTOTEST_S3} ${SERVERTOTEST}
aws s3 cp ${SERVERREPOSITORY_S3} ${SERVERREPOSITORY}

OutputReport="${TESTREPORT}_${DATECUR}.csv" && [[ -f $OutputReport ]] && rm -f OutputReport

for host in $(cat $SERVERTOTEST | sed 1d | awk -F';' '{print $1}')
do 
	status=0
	ip=$(grep -i "^$host;" $SERVERREPOSITORY | awk -F';' '{print $2}')
	echo "testing $host/$ip..." &&\
	$ROOTDIR/bin/submit.sh test $ip $host 1>>${OutputReport}
done

aws s3 cp ${OutputReport} ${TESTREPORT_S3}_${DATECUR}.csv

