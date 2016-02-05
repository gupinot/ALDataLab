#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

DATECUR=$(date +"%Y%m%d-%H%M%S")

type="linux"
for col in lsof ps
do
	tmpfile=$DIR_TOSEND/$col_$type_$(date +"%Y%m%d-%H%M%S").csv.gz
	ls $DIR_COLLECT/$col*.gz | xargs -n 100 -0 -I file cat $DIR_COLLECT/file >> $tmpfile &&\
	for fic in $(ls $DIR_COLLECT/$col*.gz)
	do
		CMD="mv $DIR_COLLECT/$fic $DIR_DONECOLLECT/." &&
		echo "$CMD" && $CMD
	done &&\
	CMD="aws s3 cp $tmpfile $S3_DIR_COLLECT/$(basename $tmpfile)" &&\
	echo "$CMD" && $CMD &&\
	mv $tmpfile $DIR_SENT/.
done
