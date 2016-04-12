#!/usr/bin/env bash

CONF=$HOME/pipeline/conf/update-env.conf

zeppelinuser=$(cat $CONF | egrep '^shell\.zeppelinuser' | awk '{print $2}')
zeppelinbucket=$(cat $CONF | egrep '^shell\.zeppelinbucket' | awk '{print $2}')


echo -e "export SPARK_SUBMIT_OPTIONS=\"--jars /root/pipeline/lib/ALDataLab-assembly-1.3.1.jar\"" >> /root/zeppelin/conf/zeppelin-env.sh
cat /root/zeppelin/conf/zeppelin-site.xml | sed -e "s/gezeppelin/${zeppelinbucket}/g" | sed -e "s/<value>zeppelin<\/value>/<value>${zeppelinuser}<\/value>/g" > /root/zeppelin/conf/zeppelin-site.xml.tmp
mv /root/zeppelin/conf/zeppelin-site.xml.tmp /root/zeppelin/conf/zeppelin-site.xml

su - zeppelin /root/zeppelin/bin/zeppelin-daemon.sh restart

chmod 777 /mnt/ephemeral-hdfs/s3

yum -y install jq

