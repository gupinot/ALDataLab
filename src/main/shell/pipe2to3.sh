#!/usr/bin/env bash

function mvfiledone() {
while read line
do
	file=$(basename $line)
	CMV="aws s3 mv $line $dirdone/$file"
	if [[ "$DRYRUN" == "-n" ]]
	then
		echo "$CMV"
	else
		$CMV
	fi
done < /dev/stdin
}

SUBMIT=/home/hadoop/script/submit.sh
CONF=/home/hadoop/conf/pipe2to3.conf
dirin=$(cat $CONF | egrep '^shell\.dirin' | awk '{print $2}')
dirdone=$(cat $CONF | egrep '^shell\.dirdone' | awk '{print $2}')
submitArg=""
FILEPATTERN=".*"
DRYRUN=""

LOGERR=$(mktemp)
echo "error log in $LOGERR"

usage="$0 [-f|--filepattern filepattern -n|--dry-run] [submitArg]"
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
aws s3 ls ${dirin}/ | egrep "\.gz$" | awk -v dirin=$dirin '{if ($1 == "PRE") {print dirin"/"$2} else {print dirin"/"$4}'} | egrep "$FILEPATTERN" >$tempfile
for var in connection_ webrequest_
do
	egrep "\/$var" $tempfile | $SUBMIT -c $CONF $DRYRUN $submitArg 2>$LOGERR | mvfiledone
done
#rm $tempfile
