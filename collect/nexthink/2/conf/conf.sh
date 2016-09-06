#!/usr/bin/env bash

ROOTDIR=/home/datalab
#ROOTDIR=/Users/guillaumepinot/Dev/Alstom/V2/VM

#S3
export S3GEDL="s3://gedatalab"
export S3IN="${S3GEDL}/in"
export S3REPOIN="${S3IN}/repo"

#Directory
export REPO="${ROOTDIR}/Repo"
export DATA="${ROOTDIR}/data"
export IN="${DATA}/in"
export OUT="${DATA}/out"
export DONE="${DATA}/done"

export INNXFILES="${IN}/NXFiles"
export DONENXFILES="${DONE}/NXFiles"
export DONENXFILESIN="${DONE}/NXFiles/in"
export OUTNXFILES="${OUT}/NXFiles"
export ANONYMIZED="${OUTNXFILES}/anonymized"
export MERGEDSPLITED="${OUTNXFILES}/merged-splited"

export INORAFILES="${IN}/oracle"
export DONEORAFILESIN="${DONE}/oracle/in"
export ORAANONYMIZED="${OUT}/oracle/anonymized"
export DONEORAANONYMIZED="${DONE}/oracle/anonymized"
export ORAS3IN="${S3GEDL}/oracle/in"

export INREPO="${IN}/Repo"
export DONEREPO="${DONE}/Repo"
export OUTREPO="${OUT}/Repo"

export DONEANONYMIZED="${DONENXFILES}/anonymized"
export DONEMERGEDSPLITED="${DONENXFILES}/merged-splited"

export DICTIONNARY="${REPO}/Dictionnary.csv"
export DICTIONNARY_HIST="${REPO}/dictionnary-hist"

export IDMFILE="${REPO}/IDM.csv"
export IDMFILES="${REPO}/IDM_*.csv.gz"
export IDMFileList=$(ls $IDMFILES | sort -t_ -k2 -r | tr '\n' ' ')
export I_ID_REF="${REPO}/I-ID.csv.gz"
export I_ID="${OUTREPO}/I-ID"

export DISTRIB_SHELL="${ROOTDIR}/bin/distribute.sh"
export NXPIPE_RSHELL="${ROOTDIR}/bin/R/NXPipeline.R"
export ORAPIPE_RSHELL="${ROOTDIR}/bin/R/ORAPipeline.R"
export ANONYMIZEIDM_RSHELL="${ROOTDIR}/bin/R/AnonymizeIDM.R"
export MERGE_SPLIT_SHELL="${ROOTDIR}/bin/merge_split.sh"
export REPO_SHELL="${ROOTDIR}/bin/repo.sh"
export ANONYMIZEIDM_SHELL="${ROOTDIR}/bin/AnonymizeIDM.sh"
export SENDMERGEDSPLITED_SHELL="${ROOTDIR}/bin/SendMergedSplited.sh"
export SENDORACLE_SHELL="${ROOTDIR}/bin/SendOracle.sh"
export R_WD="${ROOTDIR}/bin/R"
export TMP=$HOME/tmp

export http_proxy=http://10.249.49.100:3128
export https_proxy=$http_proxy

