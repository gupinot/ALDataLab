#!/usr/bin/env bash

set -e

yum -y install gcc-c++ make
curl --silent --location https://rpm.nodesource.com/setup_6.x | bash -
yum -y install nodejs
