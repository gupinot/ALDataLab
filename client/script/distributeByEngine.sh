#!/usr/bin/env bash

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : Start"
if [[ $# -lt 3 ]]
then
        echo "$0 : ERROR : $0 ServerList ScriptToCall ArgListOfScriptCalled"
        echo "$0 : Example : $0 \"0 1 2 3\" \"/home/hadoop/script/ALDataLab-client.sh\" \"s3 .todo pipeline2to3 s3://alstomlezoomerus/DATA/3-NXFile\""
        exit 1
fi
ListServer=$1
scriptToCall=$2
scriptArgList=$3

if [[ ! -f $scriptToCall ]]
then
        echo "$0 : ERROR : $scriptToCall does not exist"
        exit 1
fi

NbServer=$(echo $ListServer | awk -F ' ' '{print NF}')
for ((i=1; i<=$NbServer; i++))
do
	Server[$i]=$(echo $ListServer | awk -F ' ' -v NumRow=$i '{print $NumRow}')
done



#init ServerPID
for ((i=1; i<=$NbServer;i++))
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
	#CMD="$scriptToCall ${Server[$ServerID]} $1 $scriptArgList"
	CMD="$scriptToCall ${Server[$ServerID]} $1 $scriptArgList"
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : execute $CMD & ..."
	$CMD &
	ServerPID[$ServerID]=$!
}

for enginename in $(aws s3 ls s3://alstomlezoomerus/DATA/2-NXFile/ | awk '{print $4}' | grep -v "^$" | grep -v todo | awk -F'_' '{print $2}' | sort | uniq)
do
	runscript $enginename
done
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : End"
