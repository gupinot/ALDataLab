#!/bin/bash


echo "$(date +"%Y-%m-%d %H:%M:%S") - $0 : Begin"
Dir=$(dirname $0)
cd $Dir
exec &> >(tee -a "./logs/mainAnonymize.log")

LCKFILE=$Dir/mainAnonymize.lck
if [[ -f $LCKFILE ]]
then
	echo "$(date +"%Y-%m-%d %H:%M:%S") - $0 : $LCKFILE file found. Another process already running. Exiting"
	exit 1
else
	echo "$$" > $LCKFILE
fi

function terminate_exec(){
	echo "$(date +"%Y-%m-%d %H:%M:%S") - $0 : terminate_exec..."
	rm -f $LCKFILE
	./vbox_vm.sh stop $VersionNx
}
trap "terminate_exec" SIGINT SIGTERM

VersionNx=20
vm=$(awk -v VersionNx=$VersionNx -F';' '{if ($4 == VersionNx) print $0}' ./vbox_vm.csv | head -1)
IPVM=$(echo $vm | awk  -F';' '{print $1}')
userVM=$(echo $vm | awk  -F';' '{print $5}')

echo "$(date +"%Y-%m-%d %H:%M:%S") - $0 : starting vm..."
./vbox_vm.sh start $VersionNx
echo "$(date +"%Y-%m-%d %H:%M:%S") - $0 : vm running"

ssh $userVM@$IPVM "cd bin; ./submit.sh"

terminate_exec
echo "$(date +"%Y-%m-%d %H:%M:%S") - $0 : End"
