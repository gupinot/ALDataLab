#!/usr/bin/env bash

#parse 'DirInput' list and launch spark-submit for each file found with ToDoExt extension
if [[ $# -le 5 ]]
then
    echo "Usage : $0 InstanceName EngineOrRepoName RepoDir DirInput  DirInputFSType ToDoExt MethodName [MethodArgs]"
    echo "Example : $0 1 csv s3://S3Bucket/DATA/Repository s3://S3Bucket/DATA/Repository/in/ s3 .todo RepoProcessInFile"
    echo "Example : $0 1 sabad11478 s3://S3Bucket/DATA/Repository s3://S3Bucket/DATA/2-NXFile/ s3 .todo pipeline2to3 s3://S3Bucket/DATA/3-NXFile/"
    exit 1
fi

InstanceName=$1
EngineName=$2
D_REPO=$3
DirInput=$4
D_FS_TYPE=$5
ExtToDeal=$6
MethodName=$7
MethodArg1=""
MethodArg2=""
MethodArg3=""

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : Begin"


case $# in
    6) MethodArg1=$8
       ;;
    7) MethodArg1=$8
       MethodArg2=$9
       ;;
    8) MethodArg1=$8
       MethodArg2=$9
       MethodArg3=$10
       ;;
esac

case $D_FS_TYPE in
    "s3") FileList=$(aws s3 ls $DirInput | grep "$EngineName" | grep $ExtToDeal | awk '{print $4}')
        ;;
    "hdfs") FileList=$(hadoop fs -ls $DirInput | grep "$EngineName" | grep $ExtToDeal | awk '{print $8}')
        ;;
    *) echo "D_FS_TYPE ($D_FS_TYPE) unknown ! Exit"
        exit 1
        ;;
esac

#lock file to prevent concurrent compute on same method
LckFile=/tmp/$(basename -s .sh $0)-${MethodName}-${EngineName}.lck
if [[ -f $LckFile ]]
then
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : ERR : lock file $LckFile exists. Cannot start. Exiting."
    exit 1
else
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName)" > $LckFile
fi

for filein in $FileList
do
    fileintodeal="$(basename -s $ExtToDeal $filein)"
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : spark-submit --master yarn --driver-memory 2G --executor-memory 8G --class DLMain.DLMain file:///home/hadoop/lib/ALDataLab-assembly-1.0.jar --D_REPO ${D_REPO} --method ${MethodName} ${DirInput}${fileintodeal} ${MethodArg1} ${MethodArg2} ${MethodArg3}..."
    spark-submit --master yarn --driver-memory 2G --executor-memory 8G --class DLMain.DLMain file:///home/hadoop/lib/ALDataLab-assembly-1.0.jar --D_REPO ${D_REPO} --method ${MethodName} ${DirInput}${fileintodeal} ${MethodArg1} ${MethodArg2} ${MethodArg3}
    ret=$?
    if [[ $ret -eq 0 ]]
    then
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : spark-submit --master yarn --driver-memory 2G --executor-memory 8G --class DLMain.DLMain file:///home/hadoop/lib/ALDataLab-assembly-1.0.jar --D_REPO ${D_REPO} --method ${MethodName} ${DirInput}${fileintodeal} ${MethodArg1} ${MethodArg2} ${MethodArg3} : Done"
        #filein rm (todo file)
        case $D_FS_TYPE in
            "s3") aws s3 rm ${DirInput}${fileintodeal}${ExtToDeal}
                ;;
            "hdfs") hadoop fs -rm -f -r ${DirInput}${fileintodeal}${ExtToDeal}
                ;;
        esac
    else
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : spark-submit --master yarn --driver-memory 2G --executor-memory 8G --class DLMain.DLMain file:///home/hadoop/lib/ALDataLab-assembly-1.0.jar --D_REPO ${D_REPO} --method ${MethodName} ${DirInput}${fileintodeal} ${MethodArg1} ${MethodArg2} ${MethodArg3} : ERR"
    fi
done
rm -f $LckFile
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 ($InstanceName) : End"