#!/usr/bin/env bash

SUBMIT=$HOME/pipeline/bin/submit.sh
CONF=$HOME/pipeline/conf/pipe3to4.conf
submitArg=""
DRYRUN=""
VERBOSE=0
DATERANGE=0

LOGERR=$(mktemp)
echo "error log in $LOGERR"

usage="$0 [-n|--dry-run] [-v|--verbose] [-d|--date-range begindate endate byNbDay] [submitArg]"
while [[ $# > 0 ]]
do
key="$1"

case $key in
    -h|--help)
    echo "$usage"
    exit 0
    ;;
    -n|--dry-run)
    DRYRUN="-n"
    ;;
    -v|--verbose)
    VERBOSE=1
    ;;
    -d|--date-range)
    DATERANGE=1
    begindate=$2
    enddate=$3
    byday=$3
    shift
    shift
    shift
    ;;
    *)
    submitArg="$*"
    break
    ;;
esac
shift # past argument or value
done

tmpfile=$(mktemp)

if [[ $DATERANGE -eq 1 ]]
then
  dateto=$begindate
  while [[ "$dateto" != "$enddate" ]]
  do
    datefrom=$dateto
    echo "$datefrom" > $tmpfile
    dateto=$(date -d "$datefrom +$byday days" +"%Y-%m-%d")
    if [[ $(date -d $enddate '+%s') -lt $(date -d $dateto '+%s') ]] 
    then
      dateto=$enddate
    fi
    echo "$dateto" >> $tmpfile
    cat $tmpfile | $SUBMIT -c $CONF $DRYRUN $submitArg 2>>$LOGERR; ret=$?
  done
else
    echo "nodaterange" > $tmpfile
    cat $tmpfile | $SUBMIT -c $CONF $DRYRUN $submitArg 2>>$LOGERR; ret=$?
fi

echo "$0 : submit return : $ret"
rm $tmpfile
