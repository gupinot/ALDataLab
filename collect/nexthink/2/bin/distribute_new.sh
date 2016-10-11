#!/bin/bash

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : Start"
ServerID=$1
ListInputFile=$2
scriptToCall=$3
scriptArgList=$4

if [[ ! -f $scriptToCall ]]
then
        echo "$0 : ERROR : $scriptToCall does not exist"
        exit 1
fi

function runscript {
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : runscript() : ficin : $1"
	CMD="$scriptToCall 0 $1 $scriptArgList"
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : execute $CMD & ..."
	$CMD 
}

for fichier in $(ls -tr $ListInputFile)
do
	runscript $fichier
done


echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 $* : End"
