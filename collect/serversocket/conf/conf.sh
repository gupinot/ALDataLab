
#!/usr/bin/env bash

ROOTDIR=/datalab2/home/datalab/ALDataLab/collect/serversocket
YES=$ROOTDIR/bin/yes.sh
SCRIPT_SERVER=$ROOTDIR/bin/monitor.sh
DIR_COLLECT=/datalab3/DATA/SOCKET && [[ -d $DIR_COLLECT ]] || mkdir -p $DIR_COLLECT
DIR_DONECOLLECT=/datalab3/DATA/SOCKET/DONE && [[ -d $DIR_DONECOLLECT ]] || mkdir -p $DIR_DONECOLLECT
DIR_ERRCOLLECT=/datalab3/DATA/SOCKET/ERR && [[ -d $DIR_ERRCOLLECT ]] || mkdir -p $DIR_ERRCOLLECT
DIR_TOSEND=/datalab3/DATA/SOCKET/TOSEND && [[ -d $DIR_TOSEND ]] || mkdir -p $DIR_TOSEND
DIR_SENT=/datalab3/DATA/SOCKET/SENT && [[ -d $DIR_SENT ]] || mkdir -p $DIR_SENT
S3_DIR_COLLECT="s3://gedatalab/in/serversockets"
SERVERLIST=$ROOTDIR/conf/ServerListToCollect.csv
SERVERSTATUS=$ROOTDIR/conf/serverstatus.csv
SERVERCOLLECT=$ROOTDIR/log/servercollect.csv && [[ -d $ROOTDIR/log ]] || mkdir -p $ROOTDIR/log
CSC_DONE=$ROOTDIR/conf/datalab_csc_done.csv
