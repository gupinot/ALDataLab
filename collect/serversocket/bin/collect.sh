#!/usr/bin/env bash

HOSTNAME="HOST_NAME"
DIR_MONITOR=~/monitor && [[ -d $DIR_MONITOR ]] || mkdir -p $DIR_MONITOR
DIR_COLLECT=~/collect && [[ -d $DIR_COLLECT ]] || mkdir -p $DIR_COLLECT
MAX_SPACE_USED=1000000
MAX_SPACE_LEFT=100000
LSOF_MONITOR=${DIR_MONITOR}/lsof_${HOSTNAME}_$(date +"%Y%m%d-%H").csv.gz
PS_MONITOR=${DIR_MONITOR}/ps_${HOSTNAME}_$(date +"%Y%m%d-%H").csv.gz

export PATH=$PATH:/usr/sbin


function monitor() {
	dat=$(date +"%Y%m%d%H%M")
	sudo lsof -nPi4 | sed 1d | awk -vdat=$dat '{print dat";"$1";"$2";"$3";"$8";"$9";"$10}' | gzip -c >> $LSOF_MONITOR
	ps -Ao "%U;%p;%P;%x;%a" | sed -e "s/; */;/g" | sed -e "s/ *;/;/g" | awk -vdat=$dat -F'\n' '{print dat";"$1}' | sed 1d | gzip -c >> $PS_MONITOR 
}

function server_info() {
	echo ""
	
}

function free_space() {
	space_used=$(du -s ~ | awk '{print $1}')
	space_left=$(($(stat -f --format="%a*%S" ~)))
	([[ $space_used -gt $MAX_SPACE_USED ]] || [[ $space_left -lt $MAX_SPACE_LEFT  ]]) &&\
	 find $DIR_MONITOR -maxdepth 1 -name "*.gz" -mtime +10 | xargs rm -f
}

function collect() {
	for fic in $(ls $DIR_MONITOR | grep -v $LSOF_MONITOR | grep -v $PS_MONITOR)
	do
		mv $fic $DIR_COLLECT/.
	done
}

########################################################################################################################
#main

usage="$0 monitor|collect|free_space"

while [[ $# > 0 ]]
do
   key="$1"

   case ${key} in
     -h|--help)
        echo ${usage}
        exit 0
        ;;
     monitor|collect|free_space)
	method=$key
        ;;
    esac
    shift # past argument or value
done

case $method in
  monitor)
    #free_space &&\
    monitor
    ;;
  collect)
    collect
    ;;
  free_space)
    free_space
    ;;
esac
exit $?
