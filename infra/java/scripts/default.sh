#!/bin/bash -eux

set -e

# Install base packages
yum -y install vim curl wget unzip screen jq python-pip
