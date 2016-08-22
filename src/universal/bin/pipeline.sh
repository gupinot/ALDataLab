#!/usr/bin/env bash

#prerequesite : spark cluster created.

#script to launch under root user on master node of spark cluster created

DirLog=$HOME/pipeline/log;
if [[ ! -d "${DirLog}" ]]
then
  mkdir "${DirLog}"
fi
exec &> >(tee -a "${DirLog}/pipeline.log")

usage="$0 -d|--date-range begindate endate"

DATERANGE=0
begindate=""
enddate=""

while [[ $# > 0 ]]
do
key="$1"

case $key in
    -h|--help)
    echo "$usage"
    exit 0
    ;;
    -d|--date-range)
    DATERANGE=1
    begindate=$2
    enddate=$3
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
([[ $DATERANGE -eq 0 ]] || [[ "$begindate" == "" ]] || [[ "$enddate" == "" ]]) && (echo $usage; exit 1)

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Begin"
$HOME/pipeline/bin/syncHdfsS3.sh fromS3 &&\
$HOME/pipeline/bin/repo.sh &&\
$HOME/pipeline/bin/genAIP.sh &&\
$HOME/pipeline/bin/pipe2to3.sh &&\
$HOME/pipeline/bin/pipe3to4.sh -d $begindate $enddate 10 &&\
$HOME/pipeline/bin/pipe3to4Exec.sh -d $begindate $enddate 5 &&\
$HOME/pipeline/bin/pipe4to5.sh -d $begindate $enddate 10 &&\
$HOME/pipeline/bin/webapp.sh &&\
$HOME/pipeline/bin/encodeserversockets.sh &&\
$HOME/pipeline/bin/resolveserversockets.sh -d $begindate $enddate 2 &&\
$HOME/pipeline/bin/aggregateserversockets.sh &&\
$HOME/pipeline/bin/serverusage.sh &&\
$HOME/pipeline/bin/flow.sh &&\
$HOME/pipeline/bin/syncHdfsS3.sh toS3; ret=$?

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : end with exit code : $ret"
