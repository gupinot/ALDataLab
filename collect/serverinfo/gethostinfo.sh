#!/bin/bash

DATADIR="$HOME/content"
SCRIPTDIR="$HOME/scripts"

mkdir -p $DATADIR
cd $DATADIR
for host in $*
do
   mkdir -p $host
   $SCRIPTDIR/getserverinfo.pl $host 
done
