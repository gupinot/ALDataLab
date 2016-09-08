#!/usr/bin/env bash

set -e

apt-get update
apt-get install -y fontconfig-config fonts-dejavu-core geoip-database libexpat1 libfontconfig1 libfreetype6 libgd3 libgeoip1 libicu55 libjbig0 libjpeg-turbo8 libjpeg8 libpng12-0 libssl1.0.0 libtiff5 libvpx3 libx11-6 libx11-data libxau6 libxcb1 libxdmcp6 libxml2 libxpm4 libxslt1.1 sgml-base ucf xml-core libluajit-5.1-2 libluajit-5.1-common libluajit-5.1-dev
dpkg -i /tmp/deb/nginx-common*.deb
dpkg -i /tmp/deb/nginx-full*.deb

cp /etc/templates/sites-available/* /etc/nginx/sites-available/
cp /etc/templates/conf.d/* /etc/nginx/conf.d/
cp -rf /etc/templates/certs /etc/nginx/

cd /etc/nginx/sites-enabled/
rm /etc/nginx/sites-enabled/*
for site in ../sites-available/*
do
   ln -s $site
done   
 
