#!/usr/bin/env bash

CONF=/home/hadoop/conf/syncHdfsS3.conf

dirdatas3=$(cat $CONF | egrep '^shell\.dirdatas3' | awk '{print $2}')
dirs3temp=$(cat $CONF | egrep '^shell\.dirs3temp' | awk '{print $2}')
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
        hdfs dfs -mv ${dirins3}/connection/* ${dirpipelines3}/in/connection/. &&\
        hdfs dfs -mv ${dirins3}/webrequest/* ${dirpipelines3}/in/webrequest/. &&\
        hdfs dfs -mv ${dirins3}/repo/* ${dirpipelines3}/in/repo/. &&\
        hdfs dfs cp -p -f ${dirpipelines3}/in/connection/* ${dirinsaves3}/connection/. &&\
        hdfs dfs cp -p -f ${dirpipelines3}/in/webrequest/* ${dirinsaves3}/webrequest/. &&\
        hdfs dfs cp -p -f ${dirpipelines3}/in/repo/* ${dirinsaves3}/repo/. &&\
        s3-dist-cp --src=${dirpipelines3} --dest=${dirpipelinehdfs}
        exit $?
    ;;
    toS3)
        DATE=$(date +"%Y%m%d%H%M%S")
        hdfs dfs -mkdir ${dirpipelinesaves3}/$DATE &&\
        hdfs dfs -mv ${dirpipelines3} ${dirpipelinesaves3}/$DATE/. &&\
        s3-dist-cp --src=${dirdatahdfs} --dest=${dirpipelines3}
        exit $?
    ;;
esac

