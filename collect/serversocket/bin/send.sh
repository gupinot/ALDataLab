#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

function merge_send() {
	filein=$1
	fileout=$2
	for fic in $(cat $filein)
        do
		cat $fic >> $fileout &&\
                CMD="mv $fic $DIR_DONECOLLECT/." &&\
                echo "$CMD" && $CMD
        done &&\
        CMD="aws s3 cp $fileout $S3_DIR_COLLECT/$(basename $fileout)" &&\
        echo "$CMD" && $CMD &&\
        mv $fileout $DIR_SENT/.
}

DATECUR=$(date --utc --date "now" +"%Y%m%d-%H%M%S")
curdir=$(pwd)
cd $DIR_COLLECT

type="linux"
for col in lsof ps netstat
do
	for filedt in $(ls ${col}_*.csv.gz | cut -d_ -f3 | cut -d. -f1 | sort -u)
	do
		tmpfile=$(mktemp)
		for fic in $(ls ${col}_*_${filedt}.csv.gz)
		do
			echo $fic >> $tmpfile
		done &&\
		CMD="merge_send $tmpfile ${DIR_TOSEND}/${col}_${type}_${filedt}_${DATECUR}.csv.gz" &&\
		echo "$CMD" && $CMD
		rm -f $tmpfile
	done
done
cd $curdir
