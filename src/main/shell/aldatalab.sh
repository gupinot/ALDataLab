#!/usr/bin/env bash

function spark() {
    conf=$1
    todofiles="${@:2}"

    CLASS=$(grep 'spark.class' <$conf | awk '{print $2}')
    echo "Executing $CLASS from $JAR with config $conf for $todofiles..."
    spark-submit --class $CLASS --properties-file $conf $JAR $todofiles
    result=$?
    echo "Done."
    return $result
}


########################################################################################################################
#main

usages="$0 [-c|--conf confpath] [-j|--jar jarpath] [-d|--distribute batchfilessize] [-i|--dirin dirinpath] [-p|--fileinpattern fileinpattern] [filelist]"
CONF="/home/hadoop/conf/default.conf"
JAR="/home/hadoop/lib/default.jar"
DISTRIBUTE=$(grep 'shell.distribute' <$CONF | awk '{print $2}')
BATCHFILESIZE=$(grep 'shell.batchfilesize' <$CONF | awk '{print $2}')
RENAMEFILEACTIVE=$(grep 'shell.renamefileactive' <$CONF | awk '{print $2}')
DIRIN=$(grep 'shell.dirin' <$CONF | awk '{print $2}')
FILEINPATTERN=$(grep 'shell.fileinpattern' <$CONF | awk '{print $2}')
FILELIST=""
CURFILELIST=""

while [[ $# > 1 ]]
do
key="$1"

case $key in
    -h|--help)
    echo "$usage"
    exit 0
    ;;
    -c|--conf)
    CONF="$2"
    shift # past argument
    ;;
    -j|--jar)
    JAR="$2"
    shift # past argument
    ;;
    -d|--distribute)
    DISTRIBUTE="true"
    BATCHFILESIZE="$2"
    shift # past 1st value
    ;;
    -i|--dirin)
    DIRIN="$2"
    shift # past 1st value
    ;;
    -p|--fileinpattern)
    FILEINPATTERN="$2"
    shift # past 1st value
    ;;
    *)
    FILELIST="${@:1}"
    shift
    ;;
esac
shift # past argument or value
done


function mvFileList {
    extfrom=$1
    extto=$2
    FileList="${@:3}"
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

function initList() {
     if [[ "$RENAMEFILEACTIVE" == "true" ]]
     then
        mvFileList ".todo" ".ongoing" "$*"
     fi
}

function commitList() {
     if [[ "$RENAMEFILEACTIVE" == "true" ]]
     then
        mvFileList ".ongoing" ".done" "$*"
     fi
}

function uncommitList() {
     if [[ "$RENAMEFILEACTIVE" == "true" ]]
     then
        mvFileList ".ongoing" ".todo" "$*"
     fi
}
if [[ "$FILELIST" == "" ]]
then
    echo "nothing to do"
    exit 0
fi

TRAP "uncommitList $CURFILELIST" SIGINT SIGTERM

if [[ "$DISTRIBUTE" != "true" ]]
then
    CURFILELIST=$FILELIST
    initList $CURFILELIST
    spark $CONF $CURFILELIST
    ret=$?
    if [[ $ret -eq 0 ]]
    then
        commitList $CURFILELIST
    else
        uncommitList $CURFILELIST
    fi
else
    for batchfile in $(echo $FILELIST | xargs -n $BATCHFILESIZE | tr " " ";")
    do
        CURFILELIST=$(echo $batchfile | tr ";" " ")
        initList $CURFILELIST
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark $CURFILELIST : BEGIN"
        spark $CONF $CURFILELIST
        ret=$?
        if [[ $ret -eq 0 ]]
        then
            echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark $CURFILELIST : OK"
            commitList $CURFILELIST
        else
            echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark $CURFILELIST : KO"
            uncommitList $CURFILELIST
        fi
    done
fi