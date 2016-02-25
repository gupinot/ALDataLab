#!/usr/bin/env bash

WD=$(dirname $0)
if [[ "$WD" == "." ]]
then
	WD=$(pwd)
fi

. $WD/../conf/conf.sh

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : begin"

file=${DICTIONNARY}.lck
[[ -f $file ]] && [[ `stat --format=%Y $file` -le $(( `date +%s` - 120 )) ]] && rm -f $file
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : end"
