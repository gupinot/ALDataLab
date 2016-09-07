#!/bin/bash

Dir=$(dirname $0)
vboxvmfile=$Dir/vbox_vm.csv


function start_vm {
	versionnx=$1
	for vmid in $(awk -v versionnx=$versionnx -F';' '{if ($4 == versionnx) print $3}' $vboxvmfile)
	do
		if [[ $(vboxmanage list runningvms | grep $vmid | wc -l) -eq 0 ]]
		then
			echo "starting vm $vmid..."
			nohup vboxheadless -startvm $vmid&
		else
			echo "$vmid already started"
		fi
	done

	#wait vm running
	for vmid in $(awk -v versionnx=$versionnx -F';' '{if ($4 == versionnx) print $3}' $vboxvmfile)
	do
		vmlogin=$(awk -v versionnx=$versionnx -v vmid=$vmid -F';' '{if ($4 == versionnx && $3 == vmid) print $5}' $vboxvmfile)
		IP=$(cat $vboxvmfile | grep $vmid | awk -F";" '{print $1}')
		while ! ssh nexthink@$IP "echo OK" >/dev/null 2>/dev/null
		do
			sleep 10
		done
	done
	
}

function poweroff_vm {
	versionnx=$1
	for vmid in $(awk -v versionnx=$versionnx -F';' '{if ($4 == versionnx) print $3}' $vboxvmfile)
	do
		if [[ $(vboxmanage list runningvms | grep $vmid | wc -l) -eq 1 ]]
		then
			echo "poweroff vm $vmid..."
			vboxmanage controlvm $vmid poweroff
		else
			echo "$vmid already stopped"
		fi
	done
}

if [[ "$1" == "start" ]]
then
	start_vm $2
else
	poweroff_vm $2
fi
