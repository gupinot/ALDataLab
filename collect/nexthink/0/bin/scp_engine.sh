
DumpDir=/datalab3/DATA/0-NXProdBackups
PathFile=/var/nexthink/engine/01/backups/nxengine-backup.tgz
ServerName=$1

DATE=$(date +"%Y%m%d")
HEURE=$(date +"%H%M%S")
cd $DumpDir
echo "$DATE $HEURE scp_engine.sh : Debut de la copie $ServerName..."
scp -pB -o "StrictHostKeyChecking no" datalab@${ServerName}:${PathFile} ./${ServerName}_${DATE}.tgz.tmp
mv ./${ServerName}_${DATE}.tgz.tmp ./${ServerName}_${DATE}.tgz

DATE=$(date +"%Y%m%d")
HEURE=$(date +"%H%M%S")
echo "$DATE $HEURE scp_engine.sh : Fin de la copie $ServerName"

