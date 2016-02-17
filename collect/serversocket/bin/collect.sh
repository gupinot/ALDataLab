#!/bin/bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : Start"

[[ $# -eq 0 ]] && echo "usage : $0 \"serverlist\"" && echo "ex : $0 \"1 2 3\"" && exit 1

ListServer=$1
InputFile=$SERVERSTATUS
scriptToCall="$ROOTDIR/bin/submit.sh collect"


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
	CMD="$scriptToCall $1"
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : execute $CMD & ..."
	$CMD &
	ServerPID[$ServerID]=$!
}

for server in $(cat $InputFile | awk -F';' '{if ($3 == "2") print $2}')
do
	runscript $server
done

#wait all process terminate
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : nothing more to do. Waiting all process terminate..."
for ((i=1; i<=${#ServerPID[@]}; i++))
do
  while true;do
  	if [[ "${ServerPID[$i]}" != "" ]]
  	then
  		tmp=$(ps -p ${ServerPID[$i]} | wc -l)
  		if [[ "${tmp//[[:blank:]]/}" == "1" ]]
  		then
  			ServerPID[$i]=""
  			break
  		else
  		  echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : nothing more to do. Waiting all process terminate : ${ServerPID[$i]} still running"
  		  sleep 10
  		fi
  	else
  	  break
  	fi
  done
done

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : End"
