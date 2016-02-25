#!/usr/bin/env bash

#prerequesite : spark cluster created.
#For example : cd infra/ec2; ./spark-ec2 -k KeyLezoomerUs -i ~/.ssh/KeyLezoomerUs.pem --spot-price 0.2 --pipeline-version=1.3.1 -s 20 launch gespark

#script to launch under root user on master node of spark cluster created
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Begin"
$HOME/pipeline/bin/syncHdfsS3.sh fromS3 &&\
$HOME/pipeline/bin/repo.sh &&\
$HOME/pipeline/bin/genAIP.sh &&\
$HOME/pipeline/bin/pipe2to3.sh &&\
$HOME/pipeline/bin/encodeserversockets.sh &&\
$HOME/pipeline/bin/pipe3to4.sh &&\
$HOME/pipeline/bin/resolveserversockets.sh &&\
$HOME/pipeline/bin/pipe4to5.sh &&\
$HOME/pipeline/bin/webapp.sh &&\
$HOME/pipeline/bin/flow.sh &&\
$HOME/pipeline/bin/serverusage.sh &&\
$HOME/pipeline/bin/syncHdfsS3.sh toS3; ret=$?

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : end with exit code : $ret"
