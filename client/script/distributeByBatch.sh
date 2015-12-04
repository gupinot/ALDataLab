#!/usr/bin/env bash

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : Start"
if [[ $# -lt 5 ]]
then
        echo "$0 : ERROR : $0 BatchSize BatchFilesSize filesPattern exttodofile ScriptToCall [ArgListOfScriptCalled]"
        exit 1
fi
BatchSize=$1
BatchFilesSize=$2
filesPattern="$3"
exttodofile="$4"
ScriptToCall="$5"
ArgListOfScriptCalled="$6"

if [[ ! -f $ScriptToCall ]]
then
        echo "$0 : ERROR : $scriptToCall does not exist"
        exit 1
fi

for ((i=1; i<=$BatchSize; i++))
do
	Server[$i]=$i
done

#init ServerPID
for ((i=1; i<=$BatchSize;i++))
do
	ServerPID[$i]=""
done


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
	CMD="$ScriptToCall ${Server[$ServerID]} \"$ArgListOfScriptCalled\" \"$*\""
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : execute $CMD & ..."
	$CMD &
	ServerPID[$ServerID]=$!
}

fileList=$(aws s3 ls $(dirname $filesPattern)/ | grep $(basename $filesPattern) | grep -v folder | grep $exttodofile | awk '{if ($1 == "PRE") {print $2} else {print $4}}' | xargs basename -s "$exttodofile" | awk -v dir=$(dirname $filesPattern) '{print dir "/" $1}')

for batchfile in $(echo $fileList | xargs -n 10 | tr " " ";")
do
	runscript $(echo $batchfile | tr ";" " ")
done
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : End"