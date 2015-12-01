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
