#!/bin/bash

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : Start"
if [[ $# -lt 3 ]]
then
        echo "$0 : ERROR : paramètres attendus : $0 ServerList InputDir ListInputFileCmd ScriptToCall ArgListOfScriptCalled"
        exit 1
fi
ListServer=$1
InputDir=$2
ListInputFileCmd=$3
scriptToCall=$4
scriptArgList=$5
echo "$0 : ListServer=$ListServer; InputDir=$InputDir; ListInputFileCmd=$ListInputFileCmd; scriptToCall=$scriptToCall; scriptArgList=$scriptArgList"
echo "ListServer=$ListServer"

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
	CMD="$scriptToCall ${Server[$ServerID]} $InputDir/$1 $scriptArgList"
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : execute $CMD & ..."
	$CMD &
	ServerPID[$ServerID]=$!
}

Dir=$PWD
cd $InputDir
for fichier in $(eval $ListInputFileCmd)
do
	cd $Dir
	runscript $fichier
done
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : End"
