#!/bin/bash

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : Start"
if [[ $# -lt 3 ]]
then
        echo "$0 : ERROR : paramètres attendus : $0 ServerList DirToScan ScriptToCall ArgListOfScriptCalled"
        echo "$0 : Exemple : $0 \"192.168.17.3 192.168.18.3\" \"/Users/guillaumepinot/Dev/Alstom/Nextink/NAS/*.tgz\" /Users/guillaumepinot/Dev/Alstom/Nextink/script/install_extract.sh \"/Users/guillaumepinot/Dev/Alstom/Nextink/script/nxsql.csv /Users/guillaumepinot/Dev/Alstom/Nexthink/NAS/CsvDump\""
        exit 1
fi
ListServer=$1
ListInputFile=$2
scriptToCall=$3
scriptArgList=$4

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
		echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0/waitServerIdle() : checking ServerPID list"
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
			echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0/waitServerIdle() : waiting serverIdle..."
			sleep 10
		fi
	done
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0/waitServerIdle() serverIdle found"
	return $ServerIdle
}

function runscript {
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : runscript() : ficin : $1"
	waitServerIdle
	ServerID=$?
	CMD="$scriptToCall ${Server[$ServerID]} $1 $scriptArgList"
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : execute $CMD & ..."
	$CMD &
	ServerPID[$ServerID]=$!
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : PID : ${ServerPID[$ServerID]}"
}

for fichier in $(ls -tr $ListInputFile)
do
	runscript $fichier
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
