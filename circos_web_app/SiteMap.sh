#!/bin/sh

case "$1" in
  pulldata)
	echo "cp data from s3..."
	dest="/home/data/data_$(date +"%Y%m%dT%H%M%S")"
	mkdir $dest
	aws s3 cp s3://gewebapp/ $dest/ --recursive
	sudo service opencpu stop
	rm /home/datalab/SiteMap/datainput/4-AppliData && ln -s $dest /home/datalab/SiteMap/datainput/4-AppliData
	sudo service opencpu start
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


