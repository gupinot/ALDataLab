#!/usr/bin/env bash

MAINCLASS="com.alstom.datalab.Main"
JARFILE="file:///home/hadoop/lib/ALDataLab-assembly-1.1.jar"
SPARKSUBMIT="spark-submit --master yarn --driver-memory 1G --executor-memory 8G"

#parse 'DirInput' list and launch spark-submit for each file found with ToDoExt extension
if [[ $# -lt 5 ]]
then
    echo "Usage : $0 InstanceName RepoDir ToDoExt MethodName dirout [\"ListFiles\"]"
    echo "Example : $0 Batch1 s3://S3Bucket/DATA/Repository .todo pipeline2to3 \"s3://alstomlezoomerus/DATA/2-NXFile/webrequest_suwnd10005.dom3.ad.sys_20150613.tgz.csv.gz s3://alstomlezoomerus/DATA/2-NXFile/webrequest_suwnd10005.dom3.ad.sys_20150620.tgz.csv.gz\""
    exit 1
fi

InstanceName=$1
D_REPO=$2
ToDoExt=$3
MethodName=$4
dirout=$5
FileList="${@:6}"

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : Begin"

function mvFileList {
    extfrom=$1
    extto=$2
    if [[ "$FileList" != "" ]]
    then
        for filein in $FileList
        do
            CMD="aws s3 mv $filein.$extfrom $filein.$extto"
            $CMD
        done
    fi
}

trap "mvFileList .ongoing .todo" EXIT


mvFileList .todo .ongoing

CMD="$SPARKSUBMIT --class $MAINCLASS $JARFILE --repo $D_REPO --dirout $dirout --method ${MethodName} $FileList"
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : $CMD"
$CMD
ret=$?
if [[ $ret -eq 0 ]]
then
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : $CMD : OK"
    mvFileList .ongoing .done
else
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : $CMD : KO"
    mvFileList .ongoing .todo
fi
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : End"