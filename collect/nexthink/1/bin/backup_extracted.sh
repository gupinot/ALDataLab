#!/bin/bash
DirLog=$HOME/log;
if [[ ! -d "${DirLog}" ]]
then
        mkdir "${DirLog}"
fi
exec &> >(tee -a "${DirLog}/backup_extracted.log")

DirToBackup=/datalab3/DATA/1-NXQLProdBackups/Archives

cd $DirToBackup
for fic in $(ls *.gz)
do
	CMD="scp $fic nexthink@sfpld19901.ad.sys:/volume1/1-NXQLProdBackups/."
	echo $CMD && $CMD &&\
	mv $fic ./Backuped/.
done

