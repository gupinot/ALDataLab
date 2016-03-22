#!/usr/bin/env bash

WD=$(dirname $0)
if [[ "$WD" == "." ]]
then
	WD=$(pwd)
fi

. $WD/../conf/conf.sh


echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : begin"
########################################################
#Create updated version of I-ID csv file and send to s3
DestFile="${I_ID}_$(date +"%Y%m%d").csv"
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : DestFile = ${DestFile}"
IDMFileList=$(ls $IDMFILES | sort -t_ -k2 -r | tr '\n' ' ')
${ANONYMIZEIDM_RSHELL} "${IDMFileList}" "${DestFile}" "${I_ID_REF}"
if [[ -f ${DestFile} ]]
then
  echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : CMD : aws s3 cp ${DestFile} ${S3REPOIN}/$(basename ${DestFile})"
  aws s3 cp ${DestFile} "${S3REPOIN}/$(basename ${DestFile})"; ret=$?
  if [[ $ret -eq 0 ]]
  then
    mv ${DestFile} ${DONEREPO}/$(basename ${DestFile})
  else
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Error in CMD : aws s3 cp ${DestFile} ${S3REPOIN}/$(basename ${DestFile})"
  fi
else
  echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Error in CMD : ${ANONYMIZEIDM_RSHELL} \"${IDMFileList}\" \"${DestFile}\" \"${I_ID_REF}\""
fi

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : End"

