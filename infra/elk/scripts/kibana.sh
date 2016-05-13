#!/usr/bin/env bash

set -e

echo "Fetching Kibana..."
cd /tmp
curl -O https://download.elasticsearch.org/kibana/kibana/kibana-$KIBANA_VERSION-linux-x64.tar.gz

useradd -d /opt/kibana -M -r -U kibana
echo "Installing Kibana..."
tar xvf kibana-*.tar.gz

mkdir -p /opt/kibana
mv kibana-*/* /opt/kibana/
chown -R kibana:kibana /opt/kibana
