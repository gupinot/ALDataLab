#!/usr/bin/env bash

function spark() {
    CONF=$1

    CLASS=$(grep 'spark.class' <$CONF | awk '{print $2}')
    echo -n "Executing $CLASS from $JAR with config $CONF for $todofiles..." >&5
    spark-submit --class $CLASS --properties-file $CONF $JAR $todofiles
    result $?
    echo "Done." >&5
}

#main
usages="$0 [-c|--conf confpath] [-j|--jar jarpath] [-d|--distribute nbparrallelize batchfilessize] patternfile"
CONF=/home/hadoop/conf/default.conf
JAR=/home/hadoop/lib/default.jar
DISTRIBUTE=NO
patternfile="s3://alstomlezoomerus/DATA/2-NXFile/*.gz.todo"

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
    DISTRIBUTE=YES
    nbparrallelize="$2"
    batchfilessize="$3"
    shift # past argument
    shift # past 1st value
    ;;
    -m|--method)
    method="$2"
    shift # past argument
    ;;
    *)
    patternfile="$2"
    shift
    ;;
esac
shift # past argument or value
done

