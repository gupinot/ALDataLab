#!/bin/bash

HADOOP_NATIVE_HOME=/root/hadoop-native
VERSIONS="2.4.1 2.7.3"

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
yum install -y protobuf-compiler cmake openssl-devel snappy
for version in $VERSIONS
do
    target=${HADOOP_NATIVE_HOME}-$version
    cd /opt
    mkdir -p $target
    wget -O - "http://archive.apache.org/dist/hadoop/common/hadoop-$version/hadoop-$version-src.tar.gz" | tar xvzf -
    cd hadoop-${version}-src
    mvn package -Pdist,native -DskipTests -Dtar
    mv hadoop-dist/target/hadoop-${version}/lib/native/* ${HADOOP_NATIVE_HOME}
    ln -sf /usr/lib64/libsnappy.so.1 ${HADOOP_NATIVE_HOME}/.
    rm -rf /opt/hadoop-${version}-src
done

rm -rf /opt/apache-maven-3.2.3

