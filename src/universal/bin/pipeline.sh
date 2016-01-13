#!/usr/bin/env bash

#create cluster
#bootstrap cluster
#-

$HOME/pipeline/bin/syncHdfsS3.sh fromS3 &&\
$HOME/pipeline/bin/repo.sh &&\
$HOME/pipeline/bin/genAIP.sh &&\
$HOME/pipeline/bin/pipe2to3.sh &&\
$HOME/pipeline/bin/pipe3to4.sh &&\
$HOME/pipeline/bin/syncHdfsS3.sh toS3
