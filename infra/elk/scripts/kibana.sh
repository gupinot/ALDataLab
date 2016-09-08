#!/usr/bin/env bash

set -e

useradd -d /opt/kibana -M -r -U kibana
cd /opt
echo "Fetching Kibana..."
curl -s https://download.elasticsearch.org/kibana/kibana/kibana-$KIBANA_VERSION-linux-x64.tar.gz | tar zxf -
ln -s kibana-* kibana
chown -R kibana:kibana /opt/kibana

echo "Installing kibana plugins..."
cd /opt/kibana
bin/kibana plugin --install elastic/sense
bin/kibana plugin --install elastic/timelion
bin/kibana plugin -i sunburst -u https://github.com/rluta/kbn_sunburst_vis/releases/download/v1.0.1/kbn_sunburst_vis-1.0.1.tar.gz
bin/kibana plugin -i sankey -u https://github.com/rluta/kbn_sankey_vis/releases/download/v0.1.0/kbn_sankey_vis.zip 
bin/kibana plugin -i boxplot -u https://github.com/rluta/kbn_boxplot_violin_vis/releases/download/v1.0.0/kbn_boxplot_violin_vis-1.0.0.tar.gz
