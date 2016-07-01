#!/usr/bin/env bash

HOSTNAME="HOST_NAME"
OSTYPE="OS_TYPE"
VERSION=v2
MAX_SPACE_USED=1000000
MAX_SPACE_LEFT=100000

if [[ -f $HOME/timedelta.sh ]]
then
	. $HOME/timedelta.sh
else
	TIMEDELTA=""
fi
if [[ "$OSTYPE" == "linux" ]]; then
    datH=$(date --utc +"%Y%m%d-%H")
    datMM=$(date --utc +"%Y-%m-%dT%H:%M:00")
else
    datH=$(date -u +"%Y%m%d-%H")
    datMM=$(date -u +"%Y-%m-%dT%H:%M:00")
fi

DIR_MONITOR=~/monitor && [[ -d $DIR_MONITOR ]] || mkdir -p $DIR_MONITOR
DIR_COLLECT=~/collect && [[ -d $DIR_COLLECT ]] || mkdir -p $DIR_COLLECT

LSOF_MONITOR=${DIR_MONITOR}/${VERSION}_${OSTYPE}_lsof_${HOSTNAME}_${datH}.csv.gz
NETSTAT_MONITOR=${DIR_MONITOR}/${VERSION}_${OSTYPE}_netstat_${HOSTNAME}_${datH}.csv.gz
PS_MONITOR=${DIR_MONITOR}/${VERSION}_${OSTYPE}_ps_${HOSTNAME}_${datH}.csv.gz

export PATH=$PATH:/usr/sbin:/usr/local/bin:/usr/local/sbin

function monitor_linux() {
    ret=0
    retcmd=1
	sudo lsof -nPi | sed 1d | \
	awk -vdat=${datMM} -vdeltatime=$TIMEDELTA -vserver=$HOSTNAME -vos=$OSTYPE \
	'$8 ~ /UDP|TCP/ {print "\""os"\";\""server"\";\""dat"\";\""timedelta"\";\""$1"\";\""$2"\";\""$3"\";\""$8"\";\""$9"\";\""$10"\""}\
	 $7 ~ /UDP|TCP/ {print "\""os"\";\""server"\";\""dat"\";\""timedelta"\";\""$1"\";\""$2"\";\""$3"\";\""$7"\";\""$8"\";\""$9"\""}' | gzip -c >> $LSOF_MONITOR && retcmd=0
    [[ retcmd -ne 0 ]] && ret=$(($ret+1))

    retcmd=1
	ps -ef | sed 1d | \
	perl -pe "s/([^ ]+)\s+([^ ]+)\s+([^ ]+)\s+([^ ]+)\s+([^ ]+)\s+([^ ]+)\s+([^ ]+)\s+([^ ].*)/\"${OSTYPE}\";\"${HOSTNAME}\";\"${datMM}\";\"${TIMEDELTA}\";\"\1\";\"\2\";\"\3\";\"\7\";\"\8\"/" |\
	gzip -c >> $PS_MONITOR && retcmd=0
	[[ retcmd -ne 0 ]] && ret=$(($ret+1))

    retcmd=1
	sudo netstat --ip -anp | sed 1d | \
	awk -vdat=${datMM} -vdeltatime=$TIMEDELTA -vserver=$HOSTNAME -vos=$OSTYPE \
	'$1 ~  /udp|tcp|Udp|Tcp|UDP|TCP/ && $7 != "" {print "\""os"\";\""server"\";\""dat"\";\""deltatime"\";\""$1"\";\""$4"\";\""$5"\";\""$6"\";\""$7"\""}' | \
	gzip -c >> $NETSTAT_MONITOR && retcmd=0
	[[ retcmd -ne 0 ]] && ret=$(($ret+1))
	return $ret
}

function monitor_aix() {
    ret=0
    retcmd=1
	sudo lsof -nPi | sed 1d | \
	awk -vdat=${datMM} -vdeltatime=$TIMEDELTA -vserver=$HOSTNAME -vos=$OSTYPE \
	'$8 ~ /UDP|TCP/ {print "\""os"\";\""server"\";\""dat"\";\""timedelta"\";\""$1"\";\""$2"\";\""$3"\";\""$8"\";\""$9"\";\""$10"\""}\
	 $7 ~ /UDP|TCP/ {print "\""os"\";\""server"\";\""dat"\";\""timedelta"\";\""$1"\";\""$2"\";\""$3"\";\""$7"\";\""$8"\";\""$9"\""}' | gzip -c >> $LSOF_MONITOR && retcmd=0
    [[ retcmd -ne 0 ]] && ret=$(($ret+1))

    retcmd=1
	ps -ef | sed 1d | \
	perl -pe "s/([^ ]+)\s+([^ ]+)\s+([^ ]+)\s+([^ ]+)\s+([^ ]+)\s+([^ ]+)\s+([^ ]+)\s+([^ ].*)/\"${OSTYPE}\";\"${HOSTNAME}\";\"${datMM}\";\"${TIMEDELTA}\";\"\1\";\"\2\";\"\3\";\"\7\";\"\8\"/" |\
	gzip -c >> $PS_MONITOR && retcmd=0
	[[ retcmd -ne 0 ]] && ret=$(($ret+1))

    retcmd=1
    sudo netstat --ip -an | sed 1d | \
	awk -vdat=${datMM} -vdeltatime=$TIMEDELTA -vserver=$HOSTNAME -vos=$OSTYPE \
	'$1 ~  /udp|tcp|Udp|Tcp|UDP|TCP/ && $6 != "" {print "\""os"\";\""server"\";\""dat"\";\""deltatime"\";\""$1"\";\""$4"\";\""$5"\";\""$6"\";\""-"\""}' | \
	gzip -c >> $NETSTAT_MONITOR && retcmd=0
	[[ retcmd -ne 0 ]] && ret=$(($ret+1))
	return $ret
}

function monitor_sun() {
    monitor_aix
}

function monitor_hp() {
    monitor_aix
}

function monitor() {
    case ${OSTYPE} in
        linux)
	        monitor_linux
            ;;
        aix)
	        monitor_aix
            ;;
        sunos)
	        monitor_sun
            ;;
        hp-ux)
	        monitor_hp
            ;;
    esac
}

function server_info() {
	echo ""
}

function free_space() {
    DF=df
    [[ "$OSTYPE" == "sunos" ]] && DF=/usr/xpg4/bin/df

	space_used=$(du -s ~ | awk '{print $1}')
	space_left=$($DF -kP | awk '{print $4}')
	([[ $space_used -gt $MAX_SPACE_USED ]] || [[ $space_left -lt $MAX_SPACE_LEFT  ]]) &&\
	 find $DIR_MONITOR -maxdepth 1 -name "*.gz" -mtime +10 | xargs rm -f
	return 0
}

function collect() {
	ret=0
	for fic in $(ls $DIR_MONITOR | grep -v $(basename $LSOF_MONITOR) | grep -v $(basename $PS_MONITOR) | grep -v $(basename $NETSTAT_MONITOR))
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
