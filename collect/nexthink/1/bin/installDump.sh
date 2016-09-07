#!/bin/bash

EngineIP=$1
dumpFile=$2
if [[ ! -f $dumpFile ]]
then
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : ERROR : $dumpFile not found"
	exit 1
fi

filename=$(basename "$dumpFile")
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : copy of $dumpFile to nexthink@${EngineIP}:/home/nexthink/$filename..."
scp "$dumpFile" nexthink@${EngineIP}:/home/nexthink/$filename
ret=$?
if [[ $ret -ne 0 ]]
then 
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : error on scp. Exiting"
	exit 1
fi
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : copy of $dumpFile to nexthink@${EngineIP}:/home/nexthink/$filename OK"
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : launch remote dump install..."
ssh -t nexthink@${EngineIP} "/home/nexthink/script/install_dump.sh /home/nexthink/$filename"
ret=$?
if [[ $ret -ne 0 ]]
then 
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : error on ssh -t nexthink@${EngineIP} ... Exiting"
	exit 1
fi
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : remote dump install OK"
