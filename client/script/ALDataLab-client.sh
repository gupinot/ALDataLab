#!/usr/bin/env bash

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Begin"

#parse 'FilesInput' list and launch spark-submit for each file found with ToDoExt extension
if [[ $# -le 4 ]]
then
    echo "Usage : $0 RepoDir FilesInput FilesInputFSType ToDoExt MethodName [MethodArgs]"
    echo "Example : $0 s3://S3Bucket/DATA/Repository s3://S3Bucket/DATA/Repository/in/*.csv s3 .todo ProcessInFile"
    exit 1
fi

D_REPO=$1
FilesInput=$2
D_FS_TYPE=$3
ExtToDeal=$4
MethodName=$5
MethodArg1=""
MethodArg2=""
MethodArg3=""



case $# in
    6) MethodArg1=$6
       ;;
    7) MethodArg1=$6
       MethodArg2=$7
       ;;
    8) MethodArg1=$6
       MethodArg2=$7
       MethodArg3=$8
       ;;
esac

case $D_FS_TYPE in
    "s3") FileList=$(aws s3 ls $FilesInput | grep $ExtToDeal | awk '{print $4}')
        ;;
    "hdfs") FileList=$(hadoop fs -ls $FilesInput | grep $ExtToDeal | awk '{print $8}')
        ;;
    *) echo "D_FS_TYPE unknown ! Exit"
        exit 1
        ;;
esac

#lock file to prevent concurrent compute on same method
LckFile=/tmp/$(basename -s .sh $0)-${MethodName}.lck
if [[ -f $LckFile ]]
then
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : ERR : lock file $LckFile exists. Cannot start. Exiting."
    exit 1
else
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0" > $LckFile
fi

for filein in $FileList
do
    fileintodeal="$(basename -s $ExtToDeal $filein)"
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark-submit --master yarn --driver-memory 4G --executor-memory 8G --class DLMain.DLMain file:///home/hadoop/lib/ALDataLab-assembly-1.0.jar --D_REPO ${D_REPO} --method ${MethodName} ${FilesInput}${fileintodeal} ${MethodArg1} ${MethodArg2} ${MethodArg3}..."
    spark-submit --master yarn --driver-memory 4G --executor-memory 8G --class DLMain.DLMain file:///home/hadoop/lib/ALDataLab-assembly-1.0.jar --D_REPO ${D_REPO} --method ${MethodName} ${FilesInput}${fileintodeal} ${MethodArg1} ${MethodArg2} ${MethodArg3}
    ret=$?
    if [[ $ret -eq 0 ]]
    then
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark-submit --master yarn --driver-memory 4G --executor-memory 8G --class DLMain.DLMain file:///home/hadoop/lib/ALDataLab-assembly-1.0.jar --D_REPO ${D_REPO} --method ${MethodName} ${FilesInput}${fileintodeal} ${MethodArg1} ${MethodArg2} ${MethodArg3} : Done"
        #filein rm (todo file)
        case $D_FS_TYPE in
            "s3") aws s3 rm ${FilesInput}${fileintodeal}${ExtToDeal}
                ;;
            "hdfs") hadoop fs -rm -f -r ${FilesInput}${fileintodeal}${ExtToDeal}
                ;;
        esac
    else
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark-submit --master yarn --driver-memory 4G --executor-memory 8G --class DLMain.DLMain file:///home/hadoop/lib/ALDataLab-assembly-1.0.jar --D_REPO ${D_REPO} --method ${MethodName} ${FilesInput}${fileintodeal} ${MethodArg1} ${MethodArg2} ${MethodArg3} : ERR"
    fi
done
rm -f $LckFile
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : End"