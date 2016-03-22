#!/usr/bin/env bash

WD=$(dirname $0)
if [[ "$WD" == "." ]]
then
	WD=$(pwd)
fi

. $WD/../conf/conf.sh

if [[ $(ps -ef | grep $0 | wc -l ) -ne 3 ]]
then
  echo "already running"
  exit 0
fi

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : begin"
for var in connection webrequest
do
  for fic in $(ls ${MERGEDSPLITED}/${var}_*.gz)
  do
    if lsof $fic; then 
      #file being written
      continue
    fi
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : CMD : aws s3 cp $fic ${S3IN}/${var}/$(basename ${fic})"
    aws s3 cp $fic ${S3IN}/${var}/$(basename ${fic}); ret=$?
    if [[ $ret -eq 0 ]]
    then
      mv $fic ${DONEMERGEDSPLITED}/.
    else
      echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : CMD : aws s3 cp $fic ${S3IN}/${var}/$(basename ${fic}) : KO" >&2
    fi
  done
done
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : End"
