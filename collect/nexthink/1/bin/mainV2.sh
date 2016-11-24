#!/bin/bash

echo "$(date +"%Y-%m-%d %H:%M:%S") - $0 : Begin"
Dir=$(dirname $0)
cd $Dir

VersionNx=$1
exec &> >(tee -a "./logs/mainV2_$VersionNx.log")


LCKFILE=$Dir/main${VersionNx}.lck
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

ListIPVM=""
for vm in $(awk -v VersionNx=$VersionNx -F';' '{if ($4 == VersionNx) print $1}' ./vbox_vm.csv)
do
	ListIPVM="$vm $ListIPVM"
done
echo "$(date +"%Y-%m-%d %H:%M:%S") - $0 : starting vm..."
./vbox_vm.sh start $VersionNx
echo "$(date +"%Y-%m-%d %H:%M:%S") - $0 : vm running"

patternv5="(/sabad15034.ad.sys/ || /sacch15002.ad.sys/ || /sumhg15005.dom1.ad.sys/ || /sumhg15004.dom1.ad.sys/)"
patternv6="!${patternv5}"
if [[ "$VersionNx" == "5" ]]
then
	./distribute.sh "$ListIPVM" "/datalab3/DATA/0-NXProdBackups" "ls -tr | egrep \".*tgz$\" | awk '$patternv5'" ./install_extract.sh "./nxql_private_notsystem.csv /datalab3/DATA/1-NXQLProdBackups"
else
	./distribute.sh "$ListIPVM" "/datalab3/DATA/0-NXProdBackups" "ls -tr | egrep \".*tgz$\" | awk '$patternv6'" ./install_extract.sh "./nxql_private_notsystem.csv /datalab3/DATA/1-NXQLProdBackups" 
fi

terminate_exec
echo "$(date +"%Y-%m-%d %H:%M:%S") - $0 : end"
