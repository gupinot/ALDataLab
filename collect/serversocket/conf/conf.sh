
#!/usr/bin/env bash

ROOTDIR=/datalab2/home/datalab/ALDataLab/collect/serversocket
YES=$ROOTDIR/bin/yes.sh
SCRIPT_SERVER=$ROOTDIR/bin/collect.sh
DIR_COLLECT=/datalab3/DATA/SOCKET && [[ -d $DIR_COLLECT ]] || mkdir -p $DIR_COLLECT
