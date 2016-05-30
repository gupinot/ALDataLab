#!/usr/bin/env bash

set -e

apt-get update
apt-get install -y nginx apache2-utils

cp /etc/templates/nginx.site /etc/nginx/sites-available/default
