#!/usr/bin/env bash

WD=$(dirname $0)
if [[ "$WD" == "." ]]
then
	WD=$(pwd)
fi

. $WD/../conf/conf.sh

echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : begin"
########################################################
#Parse repo in files, validate and send to s3 (expect I-ID)
# - AIP, MDM : validate and send to s3
# - I-ID : validate and put in REPODIR
for path in $(ls ${INREPO}/*.csv)
do
  echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : Repo : $path"
  fic=$(basename $path)
  RepoName=$(echo $fic | awk -F'_' '{print $1}')
  RepoDate=$(echo $fic | awk -F'_' '{print $2}' | awk -F'.' '{print $2}')
  if [[ "${RepoName}" == "" || "${RepoDate}" == "" ]]
  then
    echo "file ${fic} : incorrect file name"
    continue
  fi
  case $RepoName in
    AIPApplication | AIPServer | AIPSoftInstance | MDM-ITC)
      aws s3 cp ${path} $S3REPOIN/$fic; ret=$?
      if [[ $ret -eq 0 ]]
      then
        mv $path ${DONEREPO}/$fic
      else
        echo "Error in CMD : aws s3 cp ${path} $S3REPOIN/$fic"
      fi
      ;;
    IDM)
      mv $path ${REPO}/$fic
      if [[ -f $IDMFILE ]]
      then
        rm -f $IDMFILE
      fi
      cp -f ${REPO}/$fic $IDMFILE
      ;;
    *)
      echo "Unknown repo name $RepoName"
      ;;
  esac
done
echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : End"

########################################################
#
