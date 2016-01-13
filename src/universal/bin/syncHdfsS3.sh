#!/usr/bin/env bash

CONF=$HOME/pipeline/conf/syncHdfsS3.conf


dirpipelines3=$(cat $CONF | egrep '^shell\.dirpipelines3' | awk '{print $2}')
dirpipelinehdfs=$(cat $CONF | egrep '^shell\.dirpipelinehdfs' | awk '{print $2}')
dirins3=$(cat $CONF | egrep '^shell\.dirins3' | awk '{print $2}')
dirinsaves3=$(cat $CONF | egrep '^shell\.dirinsaves3' | awk '{print $2}')
dirpipelinesaves3=$(cat $CONF | egrep '^shell\.dirpipelinesaves3' | awk '{print $2}')


FILEPATTERN=".*"
DRYRUN=""
VERBOSE=0
method="fromS3"

usage="$0 [-n|--dry-run] [-v|--verbose] fromS3|toS3"

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Begin"

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
        for var in connection webrequest repo
        do
            echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : fromS3 : $var : nb file to copy : $((hdfs dfs -count ${dirins3}/${var} 2>/dev/null || echo '0 0') | awk '{print $2}')"

            [[ $(hdfs dfs -count ${dirins3}/${var} 2>/dev/null | awk '{print $2}') -gt 0 ]] &&\
                (hdfs dfs -test -d ${dirpipelines3}/in/${var}|| hdfs dfs -mkdir ${dirpipelines3}/in/${var}) &&\
                hdfs dfs -mv ${dirins3}/${var}/* ${dirpipelines3}/in/${var}/. &&\
                hdfs dfs -cp -p ${dirpipelines3}/in/${var}/* ${dirinsaves3}/${var}/.
        done
        CMD="hadoop distcp ${dirpipelines3} ${dirpipelinehdfs}"
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : $CMD"
        $CMD; ret=$?
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : $CMD exit with $ret"
    ;;
    toS3)
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : toS3"
        DATE=$(date +"%Y%m%d-%H%M%S")
        hdfs dfs -mv ${dirpipelines3} ${dirpipelinesaves3}/$DATE &&\
        hadoop distcp ${dirdatahdfs} ${dirpipelines3}; ret=$?
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : hdfs dfs -mv ${dirpipelines3} ${dirpipelinesaves3}/$DATE && hadoop distcp ${dirdatahdfs} ${dirpipelines3} exit with $ret"
    ;;
esac

exit $ret
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : End"