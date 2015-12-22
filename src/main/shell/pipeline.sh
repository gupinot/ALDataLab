#!/usr/bin/env bash

#create cluster
#bootstrap cluster
#-

./shell/syncHdfsS3.sh fromS3
./shell/repo.sh
./shell/genAIP.sh
./shell/pipe2to3.sh
./shell/pipe3to4.sh
./shell/syncHdfsS3.sh toS3
