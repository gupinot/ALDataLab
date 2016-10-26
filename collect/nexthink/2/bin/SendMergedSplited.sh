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
for var in connection webrequest execution
do
  for fic in $(ls ${MERGEDSPLITED}/${var}_*.gz)
  do
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : compute $fic"
    if lsof $fic; then 
      echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : file being written"
      continue
    fi
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : CMD : aws s3 cp $fic ${S3IN}/${var}/$(basename ${fic})"
    aws s3 cp $fic ${S3IN}/${var}/$(basename ${fic}); ret=$?
    if [[ $ret -eq 0 ]]
    then
      echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : mv file in done dir"
      mv $fic ${DONEMERGEDSPLITED}/.
    else
      echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : CMD : aws s3 cp $fic ${S3IN}/${var}/$(basename ${fic}) : KO" >&2
    fi
  done
done
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : End"
