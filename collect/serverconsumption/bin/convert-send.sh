#!/usr/bin/env bash

dirin="/datalab3/DATA/SERVER"
export dirdone="/datalab3/DATA/SERVER/done"; [[ -d $dirdone ]] || mkdir -p $dirdone
dirtrash="/datalab3/DATA/SERVER/trash"; [[ -d $dirtrash ]] || mkdir -p $dirtrash
export dirout="/datalab3/DATA/SERVER-JSON"; [[ -d $dirout ]] || mkdir -p $dirout
export convert_py=/datalab2/home/datalab/ALDataLab/infra/convert.py

echo "$(date +"%Y/%m/%d-%H:%M:%S") : $0 : start"
#trash file with small file
find $dirin -maxdepth 0 -name *.xls -size -12b -exec mv -t $dirtrash {} +

#compute xls files
function compute() {
	ficout=$dirout/serverusage_$(date +"%Y%m%d-%H%M%S.%3N").json
	$convert_py -o $ficout $* &&\
	for fic in $*
	do
		mv $fic ${dirdone}/.
	done
}

export -f compute

ls $dirin/*.xls | xargs -n 10 bash -c 'compute "$@"' _
echo "$(date +"%Y/%m/%d-%H:%M:%S") : $0 : end"
