#!/bin/bash -eux

set -e

apt-get update
apt-get install -y openjdk-8-jdk

echo "Check installation"
java -version
javac -version
