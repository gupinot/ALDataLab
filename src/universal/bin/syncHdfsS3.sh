#!/usr/bin/env bash

DirLog=$HOME/pipeline/log;
if [[ ! -d "${DirLog}" ]]
then
  mkdir "${DirLog}"
fi
exec &> >(tee -a "${DirLog}/syncHdfsS3.log")

CONF=$HOME/pipeline/conf/syncHdfsS3.conf


dirpipelines3=$(cat $CONF | egrep '^shell\.dirpipelines3' | awk '{print $2}')
dircollectserverusages3=$(cat $CONF | egrep '^shell\.dircollectserverusages3' | awk '{print $2}')
dirpipelinehdfs=$(cat $CONF | egrep '^shell\.dirpipelinehdfs' | awk '{print $2}')
dirins3=$(cat $CONF | egrep '^shell\.dirins3' | awk '{print $2}')
dirinsaves3=$(cat $CONF | egrep '^shell\.dirinsaves3' | awk '{print $2}')
dirpipelinesaves3=$(cat $CONF | egrep '^shell\.dirpipelinesaves3' | awk '{print $2}')


FILEPATTERN=".*"
DRYRUN=""
VERBOSE=0
method="fromS3"

usage="$0 [-n|--dry-run] [-v|--verbose] fromS3|toS3|fromS3Simple"

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
    fromS3|toS3|fromS3Simple)
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
        DATE=$(date +"%Y%m%d-%H%M%S")
        ret=1
        CMD="hadoop distcp ${dirpipelines3}/ ${dirpipelinesaves3}/$DATE/" && echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : fromS3 : ${CMD}" && ${CMD} &&\
        CMD="aws s3 rm ${dirpipelines3/s3n:/s3:}/done/ --recursive" && echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : fromS3 : ${CMD}" && ${CMD} &&\
        CMD="aws s3 mv ${dircollectserverusages3/s3n:/s3:}/ ${dirins3/s3n:/s3:}/serverusage/ --recursive" && echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : fromS3 : ${CMD}" && ${CMD} &&\
        ret=0 &&\
        (for var in connection webrequest repo serverusage serversockets execution
        do
            echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : fromS3 : $var : nb file to copy : $((hdfs dfs -count ${dirins3}/${var} 2>/dev/null || echo '0 0') | awk '{print $2}')"

            [[ $((hdfs dfs -count ${dirins3}/${var} 2>/dev/null || echo '0 0') | awk '{print $2}') -gt 0 ]] &&\
                (hdfs dfs -test -d ${dirpipelines3}/in/${var}|| hdfs dfs -mkdir ${dirpipelines3}/in/${var}) &&\
                (CMD1="aws s3 mv ${dirins3/s3n:/s3:}/${var}/ ${dirpipelines3/s3n:/s3:}/in/${var}/ --recursive" &&\
                CMD2="aws s3 cp ${dirpipelines3/s3n:/s3:}/in/${var}/ ${dirinsaves3/s3n:/s3:}/${var}/ --recursive" &&\
                echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : fromS3 : $var $CMD1" && $CMD1 &&\
                echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : fromS3 : $var $CMD2" && $CMD2) || (false; exit)
        done
        ) || ret=1
        [[ $ret -eq 0 ]] &&\
        CMD="hadoop distcp ${dirpipelines3} ${dirpipelinehdfs}" &&\
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : $CMD" &&\
        hdfs dfs -rm -f -R ${dirpipelinehdfs} && $CMD; ret=$?
        [[ $ret -eq 0 ]] &&\
        hdfs dfs -mkdir -p ${dirpipelinehdfs}/done &&\
        for var in connection webrequest repo serverusage serversockets execution
        do
            hdfs dfs -mkdir -p ${dirpipelinehdfs}/done/$var
        done
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : fromS3 exit with $ret"
    ;;
    fromS3Simple)
        ret=0
        for dir in out repo meta metasockets
        do
            retcmd=0
            CMDRM="hdfs dfs -rm -f -R ${dirpipelinehdfs}/$dir"
            CMDCP="hadoop distcp ${dirpipelines3}/$dir/ ${dirpipelinehdfs}/$dir"
            echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : $CMDRM && $CMDCP"
            $CMDRM && $CMDCP && retcmd=$?
            [[ $retcmd -ne 0 ]] && ret=$(($ret+1))
        done
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : fromS3 exit with $ret"
    ;;
    toS3)
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : toS3"
        aws s3 rm ${dirpipelines3/s3n:/s3:} --recursive &&\
        ret=0 &&\
        (for rep in done/repo done/connection done/execution done/serversockets done/serverusage done/webrequest in meta metasockets out/aggregated  out/encoded out/resolved out/resolved_execution out/resolved_serversockets out/serverusage repo
         do
            CMD="hadoop distcp ${dirpipelinehdfs}/${rep}/* ${dirpipelines3}/${rep}/"
            echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : $CMD" && $CMD || false || exit
         done
        ) || ret=1
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : toS3 exit with $ret"
    ;;
esac

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : End"
exit $ret
