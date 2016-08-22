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
	#CMV="aws s3 mv $line $dirout/$file"
	CMV="hdfs dfs -mv $line $dirout/$file"
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
CONF=$HOME/pipeline/conf/pipe2to3.conf
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
for var in connection webrequest execution
do
    hdfs dfs -ls ${dirin}/${var}/ | egrep -o "[^ ]+\.gz$" | egrep "$FILEPATTERN" >$tempfile
	sort -t_ -k3 $tempfile | $SUBMIT -c $CONF $DRYRUN $submitArg 2>>$LOGERR | mvfiledone "${dirdone}/${var}"
done
rm $tempfile

