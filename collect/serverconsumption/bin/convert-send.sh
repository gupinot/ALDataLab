#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

echo "$(date +"%Y/%m/%d-%H:%M:%S") : $0 : start"
#trash file with small file
find $dirserverxls -maxdepth 1 -name *.xls -size -6k -exec mv -t ${dirserverxlstrash} {} +

#compute xls files
function compute() {
	ficout=$dirserverjson/serverusage_$(date +"%Y%m%d-%H%M%S.%3N").json
	$convert_py -o $ficout $* &&\
	for fic in $*
	do
		mv $fic ${dirdone}/.
	done &&\
	gzip $ficout && scp ${ficout}.gz $dirnas &&\
	mv ${ficout}.gz ${dirserverjsonsent}
}

export -f compute

ls $dirserverxls/*.xls | xargs -n 10 bash -c 'compute "$@"' _
echo "$(date +"%Y/%m/%d-%H:%M:%S") : $0 : end"
