#!/usr/bin/env bash

MAINCLASS="com.alstom.datalab.Main"
JARFILE="file:///home/hadoop/lib/ALDataLab-assembly-1.1.jar"
SPARKSUBMIT="spark-submit --master yarn --driver-memory 1G --executor-memory 8G --executor-cores 3 --num-executors 2"

#parse 'DirInput' list and launch spark-submit for each file found with ToDoExt extension
if [[ $# -lt 5 ]]
then
    echo "Usage : $0 InstanceName RepoDir MethodName dirout dircontrol [\"ListFiles\"]"
    exit 1
fi

InstanceName=$1
D_REPO=$2
MethodName=$3
dirout=$4
dircontrol=$5
FileList=${@:6}
ToDoExt=".todo"

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : Begin $*"
echo "InstanceName:$InstanceName; D_REPO:$D_REPO; ToDoExt:$ToDoExt;MethodName:$MethodName; dirout:$dirout; dircontrol:$dircontrol; FileList:$FileList"
function mvFileList {
    extfrom=$1
    extto=$2
    if [[ "$FileList" != "" ]]
    then
        for filein in $FileList
        do
            CMD="aws s3 mv $filein$extfrom $filein$extto"
            echo "CMD:$CMD"
            $CMD
        done
    fi
}

trap "mvFileList .ongoing .todo" SIGINT SIGTERM

mvFileList .todo .ongoing
controlfile="$dircontrol/$(date +"%Y%m%d-%H%M%S")-$InstanceName.txt"

CMD="$SPARKSUBMIT --class $MAINCLASS $JARFILE --repo $D_REPO --dirout $dirout --control $controlfile --method ${MethodName} $FileList"
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