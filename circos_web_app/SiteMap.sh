#!/bin/bash

LNDIR="/home/datalab/SiteMap/datainput/4-AppliData"
S3DIR="s3://gewebapp"

case "$1" in
  pulldata)
    s3newest=$(aws s3 ls ${S3DIR}/ | grep PRE | egrep -o "[^ ]+\/" | tr -d '/' | sort | tail -1)
    echo "pulldata : newest s3 remote data directory : ${s3newest}"
    localdir=$(basename $(ls -dl $LNDIR | egrep -o "[^ ]+$"))
    echo "pulldata : local data directory ($LNDIR) : $localdir"

    if [[ "${s3newest}" -gt "$localdir" ]]
    then
        echo "pulldata : s3 remote dir more recent than local data dir => pull data from s3"
        aws s3 cp ${S3DIR}/${s3newest}/ /home/data/ --recursive &&\
    	sudo service opencpu stop &&\
	    rm $LNDIR && ln -s /home/data/${s3newest} $LNDIR &&\
	    sudo service opencpu start &&\
	    echo "pulldata : pull data from s3 OK. Newest data dir is /home/data/${s3newest}"
    else
        echo "pulldata : no most recent s3 data dir found => exit"
    fi
  ;;

  clean)
	sudo rm -fr /tmp/ocpu-www-data/tmp_library/*
	sudo rm -f /home/datalab/circos/tools/tableviewer/etc/circos_20*_*.conf
	sudo rm -f /home/datalab/SiteMap/res/Work/SiteMap/circos/tmp/circos-*.png
	sudo rm -f /home/datalab/SiteMap/res/Work/SiteMap/circos/tmp/circos_*.csv
	sudo rm -f /home/datalab/SiteMap/res/Work/SiteMap/circos/tmp/statDevice_*.csv
	sudo rm -f /home/datalab/SiteMap/res/Work/SiteMap/circos/tmp/statServer_*.csv
  ;;
  *)
    echo "Usage: $0 pulldata|clean"
    exit 1
    ;;
esac


