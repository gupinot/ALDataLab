#!/bin/bash

# get total memory for instance 
mem=$(free -g | grep 'Mem:' | awk '{print $2}')

export ES_HEAP=$((mem/2))g
export ES_MASTER="false"
export ES_DATA="false"
export ES_CLUSTER_NAME="{{cluster_name}}"
export ES_DATA_DIR="/var/lib/elasticsearch"
export ES_SECURITY_GROUPS="{{security_groups}}"
export ES_AVAILABILITY_ZONES=""
export ES_AWS_REGION="us-east-1"
export ES_AWS_KEY="{{aws_key}}"
export ES_AWS_SECRET="{{aws_secret}}"
export ES_SNAPSHOT_BUCKET="gekibana"
export ES_SNAPSHOT_PATH="snapshots"

# Install certs from s3
# Update config files
/usr/local/bin/configure.sh /etc/templates/elasticsearch.yml /etc/elasticsearch/elasticsearch.yml
/usr/local/bin/configure.sh /etc/templates/default /etc/default/elasticsearch
mkdir -p /etc/elasticsearch/commands
for cmd in /etc/templates/commands/*
do
   /usr/local/bin/configure.sh $cmd /etc/elasticsearch/commands/$(basename $cmd)
done

# Install startup scripts
cp /etc/templates/kibana.conf /etc/init/
update-rc.d elasticsearch defaults

# Install ssh keys
mkdir -p /home/ubuntu/.ssh
cat >>/home/ubuntu/.ssh/authorized_keys <<EOF
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCX1ZB+xPsgEcb3CAUg7lp28ilFijn7xSeh+F2G61CgbgP+S6BjBAJSVG+Zl+vzHYKV4hoWRp3y6wBzotEzejcy8aAIxT6aAMfYSpJKBc29ZLNbtxyCc6+ndP+wZyr4JnNMVaLFBZUWuF4U9Ml2kYWJyMbokk5PhB3fx46PAm6+DyP31EWHDZBpTJjhm2dW8Qo0QftWb/38bQKjtXuzdVuYDUVvpdubYCJR0o8Aa0nOSH2BBLocVKjP9DfIn5w8oCie8E5BE0QPjHH9xLoXbLSpiPy+RjsHeTli6ACZ6iPhUIVOJK6eJ6sjV/cem5cXb1hkf5BH0+FXQJse8EexSfSH rluta
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCI2RMMz9MAYJrUuqo64qw7DVNQ8q6Si2GH/4NafdRlFkePy5L0SmPveV5R0N7vaOPK+bdiJF2MG/piRhPnhAlsOQ4+JFfZTNEC+U75ee/3rfsG/r8DiLbjF6yvCSr0fYixVkO2ST4sxZMSoKwa9ZoqMVdDwUmOTGylbnOklrpp95punGxhjzocBqKib5F3XPfmOgxRYR2FuRvSQgCGQXz5BqBCz0DrGmK5CI6Md5xnokg6aOacwe2y4NlMrCoo58c+GVX6xXWjeF/g/1MC0kiNfBxoImviOl8L7ajYMKF3MfrHdWHRRvUtWz3X4qbnOJdJra0inEV2TBC1jzC7Q5wf KeyLezoomer
EOF
chown -R ubuntu:ubuntu /home/ubuntu/.ssh
chmod 644 /home/ubuntu/.ssh/authorized_keys

killall -15 java
service elasticsearch start
start kibana

cp /usr/local/bin/snapshot-es.sh /etc/cron.weekly/snapshot-es

echo "End of kibana.sh config"
