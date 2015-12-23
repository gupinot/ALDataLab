#!/usr/bin/env bash

CONF=/home/hadoop/conf/syncHdfsS3.conf

dirdatas3=$(cat $CONF | egrep '^shell\.dirdatas3' | awk '{print $2}')
dirdatahdfs=$(cat $CONF | egrep '^shell\.dirdatahdfs' | awk '{print $2}')
FILEPATTERN=".*"
DRYRUN=""
VERBOSE=0
method="fromS3"

LOGERR=$(mktemp)
echo "error log in $LOGERR"

usage="$0 [-n|--dry-run] [-v|--verbose] fromS3|toS3"
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
    fromS3|toS3)
    method="$1"
    break
    ;;
    *)
    echo "argument not allowed"
    exit 1
    ;;
esac
shift # past argument or value
done

case $method in
    fromS3)
        s3-dist-cp --src=$dirdatas3 --dest=$dirdatahdfs
    ;;
    toS3)
        s3-dist-cp --src=${dirdatahdfs} --dest=${dirdatas3}.result
        ret=$?
        if [ $ret -eq 0 ]
        then
            hdfs dfs -rm -r -f ${dirdatas3}
            hdfs dfs -mv {$dirdatas3}.result ${dirdatas3}
    ;;
esac

