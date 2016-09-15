#!/usr/bin/env bash

set -e

rpm --import https://packages.elastic.co/GPG-KEY-elasticsearch
cat > /etc/yum.repos.d/elasticsearch.repo <<EOF
[elasticsearch-2.x]
name=Elasticsearch repository for 2.x packages
baseurl=https://packages.elastic.co/elasticsearch/2.x/centos
gpgcheck=1
gpgkey=https://packages.elastic.co/GPG-KEY-elasticsearch
enabled=1
EOF
yum -y install elasticsearch

cd /usr/share/elasticsearch
bin/plugin install mobz/elasticsearch-head
bin/plugin install royrusso/elasticsearch-HQ
yes | bin/plugin install cloud-aws
chown elasticsearch:elasticsearch -R .
chkconfig --add elasticsearch

pip install elasticsearch-curator==3.5.1
