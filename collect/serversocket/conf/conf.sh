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
SERVERCOLLECTSTATUSALL=$ROOTDIR/conf/server-collect-status.csv
SERVERCOLLECT=$ROOTDIR/log/servercollect.csv && [[ -d $ROOTDIR/log ]] || mkdir -p $ROOTDIR/log
CSC_DONE=$ROOTDIR/conf/datalab_csc_done.csv

SERVERLISTTOCOLLECT="$ROOTDIR/conf/ServerListToCollect.csv"
SERVERLISTTOCOLLECT_S3="s3://gecustomers/document/ServerSockets/ServerListToCollect.csv"

SERVERREPOSITORY="$ROOTDIR/conf/ServerRepository.csv"
SERVERREPOSITORY_S3="s3://gecustomers/document/ServerSockets/ServerRepository.csv"

SERVERSTATUS_TESTS="$ROOTDIR/conf/ServerSockets/ge_serverstatus_geTests.csv"
SERVERSTATUS_TESTS_S3="s3://gecustomers/document/ServerSockets/ge_serverstatus_geTests.csv"

SERVERSTATUS_DEPLOY="$ROOTDIR/conf/ServerSockets/ge_serverstatus_geDeploy.csv"
SERVERSTATUS_DEPLOY_S3="s3://gecustomers/document/ServerSockets/ge_serverstatus_geDeploy.csv"

SERVERSTATUS_DEPLOYHISTORY="$ROOTDIR/conf/ServerSockets/ge_serverstatus_geDeployHistory.csv"
SERVERSTATUS_DEPLOYHISTORY_S3="s3://gecustomers/document/ServerSockets/ge_serverstatus_geDeployHistory.csv"

SERVERSTATUS_COLLECTHISTORY="$ROOTDIR/conf/ServerSockets/ge_serverstatus_geCollectHistory.csv"
SERVERSTATUS_COLLECTHISTORY_S3="s3://gecustomers/document/ServerSockets/ge_serverstatus_geCollectHistory.csv"

SERVERTOTEST="$ROOTDIR/conf/serversToTest.csv"
SERVERTOTEST_S3="s3://gecustomers/document/ServerSockets/serversToTest.csv"

TESTREPORT="$ROOTDIR/conf/test_report"
TESTREPORT_S3="s3://gecustomers/document/ServerSockets/test_report"
