#!/bin/bash

HADOOP_NATIVE_HOME=/root/hadoop-native

# Install Maven (for Hadoop)
cd /tmp
wget "http://archive.apache.org/dist/maven/maven-3/3.2.3/binaries/apache-maven-3.2.3-bin.tar.gz"
tar xvzf apache-maven-3.2.3-bin.tar.gz
mv apache-maven-3.2.3 /opt/

# Edit bash profile
echo "export JAVA_HOME=/usr/lib/jvm/java-1.8.0" >> ~/.bash_profile
echo "export M2_HOME=/opt/apache-maven-3.2.3" >> ~/.bash_profile
echo "export PATH=\$PATH:\$M2_HOME/bin" >> ~/.bash_profile

source ~/.bash_profile

# Build Hadoop to install native libs
mkdir -p ${HADOOP_NATIVE_HOME}
cd /opt
yum install -y protobuf-compiler cmake openssl-devel
wget -O - "http://archive.apache.org/dist/hadoop/common/hadoop-2.4.1/hadoop-2.4.1-src.tar.gz" | tar xvzf -
cd hadoop-2.4.1-src
mvn package -Pdist,native -DskipTests -Dtar
mv hadoop-dist/target/hadoop-2.4.1/lib/native/* ${HADOOP_NATIVE_HOME}

# Install Snappy lib (for Hadoop)
yum install -y snappy
ln -sf /usr/lib64/libsnappy.so.1 ${HADOOP_NATIVE_HOME}/.

rm -rf /opt/apache-maven-3.2.3 /opt/hadoop-2.4.1-src

