#!/usr/bin/env bash

dirin="/Users/guillaumepinot/Dev/Alstom/Nextink/DATA/SERVER"
dirArchive="/Users/guillaumepinot/Dev/Alstom/Nextink/DATA/SERVER/Archives"
dirout="/Users/guillaumepinot/Dev/Alstom/Nextink/DATA/SERVER/csv"
dirs3="s3:gedatalab/data/in/serverusage"
xls2csv=/Users/guillaumepinot/Dev/xls2csv-1.06/script/xls2csv

for fic in $(ls $dirin/*.xls)
do
    fileout=$dirout/$(basename -s .xls $fic).csv
    xls2csv -x $fic -b WINDOWS-1252 -c $fileout -a UTF-8
    aws s3 cp $fileout $dirs3/$(basename -s .xls $fic).csv
done