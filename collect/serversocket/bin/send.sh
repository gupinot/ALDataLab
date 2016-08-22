#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

function merge_sockets() {
	filein=$1
	fileout=$2
	for fic in $(cat $filein)
        do
		if gunzip -t $fic
		then
			cat $fic >> $fileout &&\
                	CMD="mv $fic $DIR_DONECOLLECT/." &&\
                	echo "$CMD" && $CMD
		else
			CMD="mv $fic $DIR_ERRCOLLECT/." &&\
			echo "$CMD" && $CMD
		fi
        done
}

function send_sockets() {
	cd ${DIR_TOSEND}
	for fic in $(find . -maxdepth 1 -name "*.csv.gz")
        do
        	CMD="aws s3 cp $fic $S3_DIR_COLLECT/$(basename $fic)" &&\
        	echo "$CMD" && $CMD &&\
		CMD="mv $fic $DIR_SENT/." && echo "$CMD" && $CMD
        done 
}

DATECUR=$(date --utc --date "now" +"%Y%m%d-%H%M%S")
curdir=$(pwd)
cd $DIR_COLLECT

//version init
type="linux"
for col in lsof ps netstat
do
	for filedt in $(find . -maxdepth 1 -name "${col}_*.csv.gz" | cut -d_ -f3 | cut -d. -f1 | sort -u)
	do
		tmpfile=$(mktemp)
		for fic in $(find .  -maxdepth 1 -name "${col}_*_${filedt}.csv.gz")
		do
			echo $fic >> $tmpfile
		done &&\
		CMD="merge_sockets $tmpfile ${DIR_TOSEND}/${col}_${type}_${filedt}_${DATECUR}.csv.gz" &&\
		echo "$CMD" && $CMD
		rm -f $tmpfile
	done
done

//version v2
version="v2"
for type in linux aix hp-ux
do
    for col in lsof ps netstat
    do
        for filedt in $(ls ${version}_${type}_${col}_*.csv.gz | cut -d_ -f5 | cut -d. -f1 | sort -u)
        do
            tmpfile=$(mktemp)
            for fic in $(ls ${version}_${type}_${col}_*_${filedt}.csv.gz)
            do
                echo $fic >> $tmpfile
            done &&\
            CMD="merge_sockets $tmpfile ${DIR_TOSEND}/${version}_${type}_${col}_${filedt}_${DATECUR}.csv.gz" &&\
            echo "$CMD" && $CMD
            rm -f $tmpfile
        done
    done
done

send_sockets
cd $curdir
