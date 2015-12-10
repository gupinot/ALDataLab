#!/usr/bin/env bash


function waitServerIdle {
	ServerIdle=0
	while [[ $ServerIdle -eq 0 ]]
	do
		for ((i=1; i<=${#ServerPID[@]}; i++))
		do
			if [[ "${ServerPID[$i]}" == "" ]]
			then
				ServerIdle=$i
				break
			else
				tmp=$(ps -p ${ServerPID[$i]} | wc -l)
				if [[ "${tmp//[[:blank:]]/}" == "1" ]]
				then
					ServerIdle=$i
					ServerPID[$i]=""
					break
				fi
			fi
		done
		if [[ $ServerIdle -eq 0 ]]
		then
			sleep 10
		fi
	done
	return $ServerIdle
}

function runscript {
	waitServerIdle
	ServerID=$?
	CMD="spark ${Server[$ServerID]} $ArgListOfScriptCalled $*"
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : execute $CMD & ..."
	$CMD &
	ServerPID[$ServerID]=$!
}

function spark() {
    CONF=$1
    todofiles="${@:2}"

    CLASS=$(grep 'spark.class' <$CONF | awk '{print $2}')
    echo -n "Executing $CLASS from $JAR with config $CONF for $todofiles..." >&5
    spark-submit --class $CLASS --properties-file $CONF $JAR $todofiles
    result $?
    echo "Done." >&5
}


########################################################################################################################
#main

usages="$0 [-c|--conf confpath] [-j|--jar jarpath] [-d|--distribute nbparrallelize batchfilessize] filelist"
CONF=/home/hadoop/conf/default.conf
JAR=/home/hadoop/lib/default.jar
DISTRIBUTE=NO

while [[ $# > 1 ]]
do
key="$1"

case $key in
    -h|--help)
    echo "$usage"
    exit 0
    ;;
    -c|--conf)
    conf="$2"
    shift # past argument
    ;;
    -j|--jar)
    JAR="$2"
    shift # past argument
    ;;
    -d|--distribute)
    distribute=YES
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
    filelist="${@:1}"
    shift
    ;;
esac
shift # past argument or value
done

if [[ "$distribute" == "NO" ]]
then
    spark $conf $filelist
else
    for ((i=1; i<=nbparrallelize; i++))
    do
        Server[$i]=$i
        ServerPID[$i]=""
    done

    for batchfile in $(echo $filelist | xargs -n $batchfilessize | tr " " ";")
    do
        runscript $(echo $batchfile | tr ";" " ")
    done
fi