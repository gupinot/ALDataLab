#!/usr/bin/env bash

function mvfiledone() {
dirout=$1
if ! hdfs dfs -test -d $dirout
then
	hdfs dfs -mkdir -p $dirout
fi
while read line
do
	file=$(basename $line)
	CMV="hdfs dfs -mv $line $dirout/."
	if [[ "$DRYRUN" == "-n" ]]
	then
		echo "$CMV"
	else
		echo "$CMV"
		$CMV
	fi
done < /dev/stdin
}

SUBMIT=$HOME/pipeline/bin/submit.sh
CONF=$HOME/pipeline/conf/serverusage.conf
dirin=$(cat $CONF | egrep '^shell\.dirin' | awk '{print $2}')
dirdone=$(cat $CONF | egrep '^shell\.dirdone' | awk '{print $2}')
submitArg=""
FILEPATTERN=".*"
DRYRUN=""
VERBOSE=0

LOGERR=$(mktemp)
echo "error log in $LOGERR"

usage="$0 [-f|--filepattern filepattern] [-n|--dry-run] [-v|--verbose] [submitArg]"
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
    -f|--filepattern)
    FILEPATTERN="$2"
    shift # past argument
    ;;
    *)
    submitArg="$*"
    break
    ;;
esac
shift # past argument or value
done

tempfile=$(mktemp)
echo "tempfile=$tempfile"

hdfs dfs -ls ${dirin}/ | egrep -o "[^ ]+(\.gz|\.json)$" | egrep -o "[^\/]+$" | egrep "$FILEPATTERN" | cut -d_ -f2 | cut -d- -f1 | sort -u | awk '{print "*"$1"*json*"}' > $tempfile
cat $tempfile | awk -v dirin=${dirin} '{print dirin"/"$1}' | $SUBMIT -c $CONF $DRYRUN $submitArg 2>>$LOGERR | mvfiledone "${dirdone}"
rm $tempfile