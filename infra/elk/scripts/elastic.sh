#!/usr/bin/env bash

set -e

cd /tmp
curl -L -o elastic.deb https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/deb/elasticsearch/${ELASTIC_VERSION}/elasticsearch-${ELASTIC_VERSION}.deb
dpkg -i elastic.deb
rm elastic.deb

cd /usr/share/elasticsearch
bin/plugin install mobz/elasticsearch-head
bin/plugin install royrusso/elasticsearch-HQ
yes | bin/plugin install cloud-aws
chown elasticsearch:elasticsearch -R .
update-rc.d elasticsearch defaults
