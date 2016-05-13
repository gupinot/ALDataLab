#!/bin/bash -eux

set -e

apt-get update
apt-get install -y openjdk-7-jdk

echo "Check installation"
java -version
javac -version
