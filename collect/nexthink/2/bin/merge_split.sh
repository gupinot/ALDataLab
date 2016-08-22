#!/usr/bin/env bash

WD=$(dirname $0)
if [[ "$WD" == "." ]]
then
	WD=$(pwd)
fi

. $WD/../conf/conf.sh

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : begin"

if [ $# -gt 0 ];then PATTERN="$1";else PATTERN="";fi

function mergefiledt() {
	filedate=$1
	tmpfile=$2

	TMPRESULT="${TMP}/$(basename $(mktemp))"
	tempdir="${TMP}"
	echo -e "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : filedate=${filedate}\n\tTMPRESULT=${TMPRESULT}\n\ttmpfile=${tmpfile}"
	for filein in $(grep "${filedate}" $tmpfile | sed "s/\\.gz$//")
	do
		echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : file : ${filein}"
		HEADER=$(gunzip -c $ANONYMIZED/${filein}.gz | head -1) 
		gunzip -c $ANONYMIZED/${filein}.gz | tail -n +2 >> $TMPRESULT || return 1
	done
	echo "${HEADER}" > $TMPRESULT.1 && cat $TMPRESULT >> $TMPRESULT.1 && mv $TMPRESULT.1 $TMPRESULT || return 1
	concatfile="${tempdir}/${var}_engine_${filedate}_$(date +"%Y%m%d-%H%M%S").csv"
	mv $TMPRESULT $concatfile || return 1
	len=$(wc -l $concatfile | awk '{print $1}')
	if [[ $len -gt 10000000 ]]
	then
	  wd=$(pwd)
		cd $tempdir
		splitlen=$((len/4+1))
		prefix="$(basename $concatfile).split."
		split -l $splitlen $(basename $concatfile) $prefix || return 1
		for splitfile in $(ls $prefix*)
		do
			if [ "${splitfile}" != "${prefix}aa" ]
			then
				echo "${HEADER}" > $splitfile.1 || return 1
				cat $splitfile >> $splitfile.1 || return 1
				mv $splitfile.1 $splitfile || return 1
			fi
			echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : gzip ${splitfile}"
			gzip $splitfile || return 1
			echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : mv ${splitfile}.gz ${MERGEDSPLITED}/."
			mv $splitfile.gz $MERGEDSPLITED/. || return 1
		done
		rm $concatfile
		cd $wd
	else
		echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : gzip ${concatfile}"
		gzip $concatfile || return 1
		mv $concatfile.gz $MERGEDSPLITED/. || return 1
	fi
	
	for filein in $(grep "${filedate}" $tmpfile | sed "s/\\.gz$//")
	do
	  echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : mv ${ANONYMIZED}/${filein}.gz ${DONEANONYMIZED}/."
		mv ${ANONYMIZED}/${filein}.gz ${DONEANONYMIZED}/.
	done
}


TMPFILE="${TMP}/$(basename $(mktemp))"
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : TMPFILE=${TMPFILE}"
for var in connection webrequest execution
do
	ls $ANONYMIZED | grep "${var}" | grep "${PATTERN}" > ${TMPFILE}_${var}
	for filedt in $(cat ${TMPFILE}_${var} | cut -d_ -f3 | cut -d. -f1 | sort -u)
	do
		mergefiledt $filedt ${TMPFILE}_${var}
	done
done

rm -f ${TMPFILE}*

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : End"
