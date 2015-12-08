#!/usr/bin/env bash

D_ROOT=s3://alstomlezoomerus
D_REPO=$D_ROOT/DATA/Repository
D_DATA_2=$D_ROOT/DATA/2-NXFile
D_DATA_3=$D_ROOT/DATA/3-NXFile
D_DATA_4=$D_ROOT/DATA/4-NXFile

echo "Repo :"
echo "/tnumber of files to pipe : $(aws s3 ls $D_REPO/in/ | grep -v folder | grep todo | wc -l)"

echo "Data level 2 :"
echo "\tnumber of files : $(aws s3 ls $D_DATA_2/ | grep -v folder | grep -v todo | wc -l)"
echo "\tnumber of files to pipe : $(aws s3 ls $D_DATA_2/ | grep -v folder | grep todo | wc -l)"

echo "Data level 3 :"
echo "\tnumber of files : $(aws s3 ls $D_DATA_3/ | grep -v folder | grep -v todo | wc -l)"
echo "\tnumber of files to pipe : $(aws s3 ls $D_DATA_3/ | grep -v folder | grep todo | wc -l)"

echo "Data level 4 :"
echo "\tnumber of files : $(aws s3 ls $D_DATA_4/ | grep -v folder | grep -v todo | wc -l)"
echo "\tnumber of files to pipe : $(aws s3 ls $D_DATA_4/ | grep -v folder | grep todo | wc -l)"

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