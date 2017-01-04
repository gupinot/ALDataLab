#!/bin/bash

#- script main : 
# - input : 
#	- fichier csv contenant la liste des requêtes NxSQL à traiter
#		- colonne 1 : nom de la requête
#		- colonne 2 : requête nxsql (avec paramètres sur les date de début et fin)
#	- epertoire où se trouvent les dump disponibles et à traiter
#	- Repertoire où écrire les résultats
#	- Liste des IP des serveurs engine disponibles pour traiter les dumps
# - output : fichiers csv et par NxSQL à traiter. Nom du fichier : NomRequête_NomDump.csv

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : starting of command $*..."
DateBegin=$(date +"%Y-%m-%d %H:%M:%S")

if [[ $# -lt 4 ]]
then
	echo "$0 : ERROR : paramètres attendus : $0 NXEngineIP DumpFile nxsqlCSVFile ResDir"
	echo "$0 : Exemple : $0 "192.168.17.3" /home/nextink/NAS/sabad11479.ad.sys_20150602.tgz /home/nexthink/script/nxsql.csv /home/nexthink/CsvDump"
	exit 1
fi

EngineIP=$1
DumpFile=$2
nxsqlCSVFile=$3
ResDir=$4

if [[ ! -f $nxsqlCSVFile ]]
then
	echo "$0 : ERROR : $nxsqlCSVFile does not exist"
	exit 1
fi
if [[ ! -f $DumpFile ]]
then
	echo "$0 : ERROR : $DumpFile does not exist"
	exit 1
fi
if [[ ! -d $ResDir ]]
then
	echo "$0 : ERROR : $ResDir does not exist"
	exit 1
fi

#Lecture du fichier des requêtes à executer
i=-1
while read line
do
	i=$i+1
	NomReq[$i]=$(echo $line | awk -F';' '{print $1}')
	Req[$i]=$(echo $line | awk -F';' '{print $2}')
done < $nxsqlCSVFile

function affNxsqlCsvFile {
	for ((j=0; j < ${#Req[@]}; j++))
	do
		echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 - Req[$j] = ${Req[$j]}"
	done
}
#affNxsqlCsvFile

#install du dump
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 - cmd ./installDump.sh $EngineIP ${DumpFile}..."
./installDump.sh $EngineIP ${DumpFile}
ret=$?
if [[ $ret -ne 0 ]]
then
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : error on installDump.sh. Exiting"
	exit 1
fi

#Traitement de chaque requête
for ((j=0; j<${#Req[@]}; j++))
do
	nxSQLReqName=${NomReq[$j]}
	ouputFileName="${nxSQLReqName}_$(basename ${DumpFile}).csv"
	nxSQLReq=${Req[$j]}
		
	#execution de la requête
	echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 - cmd : ./nxqlquery.pl $EngineIP \"$nxSQLReq\" ${ResDir}/${ouputFileName} csv..."
	unset http_proxy
	unset https_proxy
	./nxqlquery.pl $EngineIP "$nxSQLReq" ${ResDir}/${ouputFileName} csv
	ret=$?
	if [[ $ret -ne 0 ]]
	then
		echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : error on nxqlquery.pl. delete previous generated files..."
		#suppression de tous les précédents fichiers générés
		for ((k=0; k<j; k++))
		do
			nxSQLReqName=${NomReq[$k]}
			ouputFileName="${nxSQLReqName}_$(basename ${DumpFile}).csv.gz"
			CMD="rm -f ${ResDir}/${ouputFileName}" && echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : $CMD" && $CMD
		done
		echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : error on nxqlquery.pl. Exit"
		exit 1
	fi
	#gzip file
	gzip -f ${ResDir}/${ouputFileName}
done

#Déplacement du Dump dans le repertoire Archives
mv ${DumpFile} "$(dirname $DumpFile)/Archives/."

DateEnd=$(date +"%Y-%m-%d %H:%M:%S")
echo "$(basename ${DumpFile});${EngineIP};${DateBegin};${DateEnd}" >> ${ResDir}/performance.csv
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : end of command $*"
