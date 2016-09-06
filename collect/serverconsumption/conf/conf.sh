#!/usr/bin/env bash

ROOTDIR=/datalab2/home/datalab/ALDataLab/collect/serverconsumption
SERVERLST=$ROOTDIR/conf/server.lst
URL="http://iww.dcs.itssc.alstom.com/nrtmd/streamsdump/server"
dirserverxls="/datalab3/DATA/SERVER"
LOG=$ROOTDIR/log; [[ -d $LOG ]] || mkdir -p $LOG

dirin="/datalab3/DATA/SERVER"
export dirdone="/datalab3/DATA/SERVER/done"; [[ -d $dirdone ]] || mkdir -p $dirdone
dirserverxlstrash="/datalab3/DATA/SERVER/trash"; [[ -d $dirserverxlstrash ]] || mkdir -p $dirserverxlstrash
export dirserverjson="/datalab3/DATA/SERVER-JSON"; [[ -d $dirserverjson ]] || mkdir -p $dirserverjson
export dirserverjsonsent="$dirserverjson/sent"; [[ -d ${dirserverjsonsent} ]] || mkdir -p ${dirserverjsonsent}
export convert_py=$ROOTDIR/../../infra/influxdb/convert.py
export dirnas="admin@10.249.49.99:/volume1/homes/datalab/serverusage"
