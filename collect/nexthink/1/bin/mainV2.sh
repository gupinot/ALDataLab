#!/bin/bash

Dir=$(dirname $0)
VersionNx=$1

patternv5="(/sabad15034.ad.sys/ || /sacch15002.ad.sys/ || /sumhg15005.dom1.ad.sys/ || /sumhg15004.dom1.ad.sys/)"
patternv6="!${patternv5}"
cd $Dir
LCKFILE=$Dir/main${VersionNx}.lck
if [[ -f $LCKFILE ]]
then
	echo "$LCKFILE file found. Another process already running. Exiting"
	exit 1
else
	echo "$$" > $LCKFILE
fi

trap "rm -f $LCKFILE" SIGINT SIGTERM

ListIPVM=""
for vm in $(awk -v VersionNx=$VersionNx -F';' '{if ($4 == VersionNx) print $1}' ./vbox_vm.csv)
do
	ListIPVM="$vm $ListIPVM"
done
echo "starting vm..."
./vbox_vm.sh start $VersionNx
echo "vm running"
if [[ "$VersionNx" == "5" ]]
then
	./distribute.sh "$ListIPVM" "/datalab3/DATA/0-NXProdBackups" "ls -tr | egrep \".*tgz$\" | awk '$patternv5'" ./install_extract.sh "./nxql_private_notsystem.csv /datalab3/DATA/1-NXQLProdBackups" 2>&1 | tee -a distribute${VersionNx}.log 
else
	./distribute.sh "$ListIPVM" "/datalab3/DATA/0-NXProdBackups" "ls -tr | egrep \".*tgz$\" | awk '$patternv6'" ./install_extract.sh "./nxql_private_notsystem.csv /datalab3/DATA/1-NXQLProdBackups" 2>&1 | tee -a distribute${VersionNx}.log 
fi
./vbox_vm.sh stop $VersionNx
rm -f $LCKFILE
