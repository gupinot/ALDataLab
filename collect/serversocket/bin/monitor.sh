#!/usr/bin/env bash

HOSTNAME="HOST_NAME"
if [[ -f $HOME/timedelta.sh ]]
then
	. $HOME/timedelta.sh
else
	TIMEDELTA=""
fi
datH=$(date --utc --date "now $TIMEDELTA" +"%Y%m%d-%H")
datM=$(date --utc --date "now $TIMEDELTA" +"%Y%m%d-%H%M")
DIR_MONITOR=~/monitor && [[ -d $DIR_MONITOR ]] || mkdir -p $DIR_MONITOR
DIR_COLLECT=~/collect && [[ -d $DIR_COLLECT ]] || mkdir -p $DIR_COLLECT
MAX_SPACE_USED=1000000
MAX_SPACE_LEFT=100000
LSOF_MONITOR=${DIR_MONITOR}/lsof_${HOSTNAME}_${datH}.csv.gz
PS_MONITOR=${DIR_MONITOR}/ps_${HOSTNAME}_${datH}.csv.gz

export PATH=$PATH:/usr/sbin


function monitor() {
	sudo lsof -nPi4 | sed 1d | awk -vdat=$datM -vserver=$HOSTNAME '{print "\""server"\";\""dat"\";\""$1"\";\""$2"\";\""$3"\";\""$8"\";\""$9"\";\""$10"\""}' | gzip -c >> $LSOF_MONITOR
	 ps -Ao "\"%U\"|||\"%p\"|||\"%P\"|||\"%x\"|||\"%a\"|||" | sed -e "s/\"|||\" */\"|||\"/g" | sed -e "s/ *\"|||\"/\"|||\"/g" | sed -e "s/ *\"|||/\"|||/g" | awk -vdat=$datM -vserver=$HOSTNAME -F'\n' '{print "\""server"\"|||\""dat"\"|||"$1}' | sed 1d | sed -e "s/|||/;/g" | sed -e "s/;$//" | gzip -c >> $PS_MONITOR 
}

function server_info() {
	echo ""
	
}

function free_space() {
	space_used=$(du -s ~ | awk '{print $1}')
	space_left=$(($(stat -f --format="%a*%S" ~)))
	([[ $space_used -gt $MAX_SPACE_USED ]] || [[ $space_left -lt $MAX_SPACE_LEFT  ]]) &&\
	 find $DIR_MONITOR -maxdepth 1 -name "*.gz" -mtime +10 | xargs rm -f
	return 0
}

function collect() {
	ret=0
	for fic in $(ls $DIR_MONITOR | grep -v $(basename $LSOF_MONITOR) | grep -v $(basename $PS_MONITOR))
	do
		mv $DIR_MONITOR/$fic $DIR_COLLECT/. && ret=0
	done
	return $ret
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
    free_space &&\
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
