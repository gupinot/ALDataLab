#!/usr/bin/env bash

D_ROOT=s3://alstomlezoomerus
D_REPO=$D_ROOT/DATA/Repository
D_DATA_2=$D_ROOT/DATA/2-NXFile
D_CONTROL=$D_ROOT/DATA/2-control

echo "Repo :"
echo "  number of files todo : $(aws s3 ls $D_REPO/in/ | grep "todo$" | wc -l)"

echo "Data level 2 :"
echo "  number of files : $(aws s3 ls $D_DATA_2/ | grep "gz$" | wc -l)"
echo "  number of files todo : $(aws s3 ls $D_DATA_2/ | grep "todo$" | wc -l)"
echo "  number of files ongoing : $(aws s3 ls $D_DATA_2/ | grep "ongoing$" | wc -l)"
echo "  number of files done : $(aws s3 ls $D_DATA_2/ | grep "done$" | wc -l)"
echo "  number of control files : $(aws s3 ls $D_CONTROL/ | grep ".csv/$" | wc -l)"


#divers shell
function lstpipegap {
    DIRROOT="s3://alstomlezoomerus"
    DIRIN="$DIRROOT/DATA/2-NXFile/"
    DIRCONTROL="$DIRROOT/DATA/2-control/"
    DIROUT="$DIRROOT/DATA/2-out/"


    filein=$(aws s3 ls $DIRIN | grep -v folder | grep -v todo | grep -v done | grep -v going | awk '{if ($1 == "PRE") {print $2} else {print $4}}' | sed "s/\/$//" )
    filetodo=$(aws s3 ls $DIRIN | grep -v folder | grep todo| awk '{if ($1 == "PRE") {print $2} else {print $4}}' | sed "s/\.todo\/$//" | sed "s/\.todo$//")
    fileongoing=$(aws s3 ls $DIRIN | grep -v folder | grep ongoing | awk '{if ($1 == "PRE") {print $2} else {print $4}}' | sed "s/\.ongoing\/$//" | sed "s/\.ongoing$//")
    filedone=$(aws s3 ls $DIRIN | grep -v folder | grep done | awk '{if ($1 == "PRE") {print $2} else {print $4}}' | sed "s/\.done\/$//" | sed "s/\.done$//")
    filecontrol=$(aws s3 ls $DIRCONTROL | grep -v folder | awk '{if ($1 == "PRE") {print $2} else {print $4}}' | sed "s/\/$//")

    ResFileDone=""
    ResFileNotTodo=""
    for filein in $filedone
    do
        if echo $filetodo | grep -q $filein; then
            ResFileTodo="$ResFileNotTodo $filein"
        else
            echo "$filein done"
            ResFileDone="$ResFileDone $filein"
        fi
    done

}
