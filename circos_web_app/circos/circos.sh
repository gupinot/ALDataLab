#!/bin/bash

exec 1>>/home/datalab/logs/circos.log
exec 2>&1

#prerequisite : circos with tableviewer installed.
# montage tools
#circos table viewer directory :
TABLEVIEWERDIR="/home/datalab/circos/tools/tableviewer"

#Perl executable path name
PERL=/opt/local/bin/perl

#Input parameters
DataIn=$1
RandomString=$2
FileOut=$3
Title="$4"

cd $TABLEVIEWERDIR
DataDir="data$RandomString"
mkdir $DataDir
cat "$DataIn" | bin/parse-table | bin/make-conf -dir "$DataDir/" 
#cat "$DataIn" | bin/parse-table | bin/make-conf -dir data/
sed -e "s/data/$DataDir/g" etc/circos_base.conf > etc/circos_${RandomString}.conf
$PERL ../../bin/circos -nosvg -param random_string=$RandomString -conf etc/circos_${RandomString}.conf
rm -fr $DataDir etc/circos_$RamdomString.conf

montage -label "$Title" results/circos-table-$RandomString-large.png -font Arial -pointsize 30 -geometry +0+100 -background White "$FileOut"
rm results/circos-table-$RandomString-large.png
cd -
