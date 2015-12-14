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

usage=
CONF="/home/hadoop/conf/default.conf"
JAR="/home/hadoop/lib/default.jar"
BATCHFILESIZE=$(grep 'shell.batchfilesize' <${CONF} | awk '{print $2}')
DRYRUN=0

while [[ $# > 1 ]]
do
   key="$1"

   case ${key} in
        -h|--help)
        echo <<EOF
$0 [-c|--conf confpath] [-j|--jar jarpath] [-d|--distribute batchfilessize] [filename filename... | - ]
EOF
        exit 0
        ;;
        -c|--conf)
        CONF="$2"
        shift # past argument
        ;;
        -j|--jar)
        JAR="$2"
        shift # past argument
        ;;
        -n|--dry-run)
        DRYRUN=1
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
   cat <&0 >${filelist}
else
   for f in $@
   do
     echo ${f} >${filelist}
   done
fi

# if we need to batch, split original file otherwise keep it a single split
if [[ ${BATCHFILESIZE} -gt 0 ]]
then
    prefix=$(basename "${filelist}.split.")
    dir=$(dirname ${filelist})
    cd ${dir} && split -l ${BATCHFILESIZE} ${filelist} ${prefix}
    splits=$(ls ${dir} | grep ${prefix})
else
    splits=${filelist}
fi

# now run the spark command for each split
for batchfile in ${splits}
do
    echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark $batchfile : BEGIN"
    if [[ ${DRYRUN} -eq 0 ]]
    then
        spark ${CONF} ${batchfile}
        ret=$?
    else
        ret=0
    fi

    if [[ ${ret} -eq 0 ]]
    then
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark $batchfile : OK"
        for f in $(cat ${batchfile})
        do
            echo ${f} >&5
        done
    else
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : spark $batchfile : KO"
    fi
done

# clean up
rm -f ${filelist} ${filelist}.split.*
