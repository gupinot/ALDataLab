#!/bin/bash -eux

set -e

# Updating and Upgrading dependencies
apt-get update 
# Install base packages
apt-get install -y vim curl wget unzip screen jq awscli python-pip
chmod 755 /tmp/bin/*
mv /tmp/bin/* /usr/local/bin
rmdir /tmp/bin
cp -rf /tmp/templates /etc/templates
# report some server info
df -m
