#!/usr/bin/env bash

set -e

curl -s https://deb.nodesource.com/gpgkey/nodesource.gpg.key | apt-key add -
echo 'deb https://deb.nodesource.com/node_6.x trusty main' > /etc/apt/sources.list.d/nodesource.list

apt-get update
apt-get install -y nginx apache2-utils

cp /etc/templates/nginx.site /etc/nginx/sites-available/default
