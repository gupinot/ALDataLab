#!/bin/bash -eux

set -e

# Install base packages
yum -y install vim curl wget unzip screen jq python-pip
chmod 755 /tmp/bin/*
mv /tmp/bin/* /usr/local/bin
rmdir /tmp/bin
cp -rf /tmp/templates /etc/templates
# report some server info
df -m
