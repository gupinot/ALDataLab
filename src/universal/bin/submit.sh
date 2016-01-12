#!/usr/bin/env bash

exec 5>&1
exec >&2

function spark() {
    conf=$1
    todofiles="$2"

    sparkargs=$(cat ${conf} | egrep '^submit ' | sed 's/submit //')
    cmdargs=$(cat ${conf} | egrep '^args ' | sed 's/args //')
    CLASS=$(grep -- '--class' <${conf} | awk '{print $3}')
    echo "Executing $CLASS from $JAR with config $conf for $todofiles..."
    spark-submit ${sparkargs} ${JAR} ${cmdargs} --filelist ${todofiles}
    result=$?
    echo "Done."
    return ${result}
}

########################################################################################################################
#main

usage="$0 [-c|--conf confpath] [-j|--jar jarpath] [-d|--distribute batchfilessize] [filename filename... | - ]"

DEFAULT_ACTION="pipeline2to3"
if [ -r "$HOME/pipeline" ]
then
  BASEDIR=$HOME/pipeline
else
  BASEDIR=$(pwd)
fi

CONF=$(egrep -l -r "^args --method ${DEFAULT_ACTION}" ${BASEDIR}/conf |head -1)
JAR=$(find $BASEDIR/lib -name '*.jar' | tail -1)
BATCHFILESIZE=$(grep 'shell.batchfilesize' <${CONF} |awk '{print $2}')
DRYRUN=0
VERBOSE=0
S3=${S3:-s3}

while [[ $# > 0 ]]
do
   key="$1"

   case ${key} in
     -h|--help)
        echo ${usage}
        exit 0
        ;;
     -c|--conf)
        CONF="$2"
        BATCHFILESIZE=$(grep 'shell.batchfilesize' <${CONF} | awk '{print $2}')
        shift # past argument
        ;;
     -j|--jar)
        JAR="$2"
        shift # past argument
        ;;
     -n|--dry-run)
        DRYRUN=1
        ;;
     -v|--verbose)
        VERBOSE=$((${VERBOSE} + 1))
        ;;
     -vv|--very-verbose)
        VERBOSE=$((${VERBOSE} + 2))
        ;;
     -d|--distribute)
        BATCHFILESIZE="${2:-0}"
        shift # past 1st value
        ;;
    esac
    shift # past argument or value
done

# Read all filenames to process
filelist=$(mktemp)
if [ "u$*" = "u" -o "$*" = "-" ]
then
   cat <&0 | sed -e "s/s3:/$S3:/" >${filelist}
else
   for f in $@
   do
     echo ${f} | sed -e "s/s3:/$S3:/" >${filelist}
   done
fi

if [[ ${VERBOSE} -gt 2 ]]
then
   echo "Files to process:"
   cat ${filelist}
fi

# if we need to batch, split original file otherwise keep it a single split
if [[ ${VERBOSE} -gt 0 ]]
then
   echo "Batch size: ${BATCHFILESIZE}"
fi
if [[ ${BATCHFILESIZE} -gt 0 ]]
then
    prefix=$(basename "${filelist}.split.")
    dir=$(dirname ${filelist})
    cd ${dir} && split -l ${BATCHFILESIZE} ${filelist} ${prefix}
    splits=$(ls ${dir} | grep ${prefix})
else
    splits=${filelist}
fi
if [[ ${VERBOSE} -gt 0 ]]
then
   echo "Splits: ${splits}"
fi

# now run the spark command for each split
for batchfile in ${splits}
do
    if [[ ${VERBOSE} -gt 0 ]]
    then
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark $batchfile : BEGIN"
    fi
    if [[ ${VERBOSE} -gt 1 ]]
    then
       echo "Files in batch $batchfile:"
       cat ${batchfile}
    fi
    if [[ ${DRYRUN} -eq 0 ]]
    then
        spark ${CONF} ${batchfile}
        ret=$?
    else
        ret=0
    fi

    if [[ ${ret} -eq 0 ]]
    then
        if [[ ${VERBOSE} -gt 0 ]]
        then
            echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark $batchfile : OK"
        fi
        for f in $(cat ${batchfile})
        do
            echo ${f} | sed -e "s/$S3:/s3:/" >&5
        done
    else
        if [[ ${VERBOSE} -gt 0 ]]
        then
            echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark $batchfile : KO"
        fi
    fi
done

# clean up
rm -f ${filelist} ${filelist}.split.*

