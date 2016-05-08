#!/bin/bash

# get total memory for instance 
mem=$(free -g | grep 'Mem:' | awk '{print $2}')

export ES_HEAP=$((mem/2))G
export ES_MASTER="true"
export ES_DATA="true"
export ES_CLUSTER_NAME="{{cluster_name}}"
export ES_DATA_DIR=["/data/sdb","/data/sdc"]
export ES_SECURITY_GROUPS="{{security_groups}}"
export ES_AVAILABILITY_ZONES=""
export ES_AWS_REGION="us-east-1"
export ES_AWS_KEY="{{aws_key}}"
export ES_AWS_SECRET="{{aws_secret}}"
export ES_SNAPSHOT_BUCKET="aldatalabtest"
export ES_SNAPSHOT_PATH="snapshots"

# Configure elasticsearch
/usr/local/bin/configure.sh /etc/templates/elasticsearch.yml /etc/elasticsearch/elasticsearch.yml
/usr/local/bin/configure.sh /etc/templates/default /etc/default/elasticsearch

# Mount data stores
mkdir -p /data/sdb /data/sdc
umount /dev/xvdb
mount /dev/xvdb /data/sdb
mount /dev/xvdc /data/sdc
chown -R elasticsearch:elasticsearch /data/*

# Define startup
update-rc.d elasticsearch defaults
update-rc.d nginx disable -f

killall -15 java
service elasticsearch start

echo "End of elastic.sh config"
