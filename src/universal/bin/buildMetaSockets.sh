#!/usr/bin/env bash
#script to rebuild meta data from pipeline/out/ sub-directories content

SUBMIT=$HOME/pipeline/bin/submit.sh
CONF=$HOME/pipeline/conf/buildMetaSockets.conf
submitArg=""
DRYRUN=""
VERBOSE=0

LOGERR=$(mktemp)
echo "error log in $LOGERR"

usage="$0 [-n|--dry-run] [-v|--verbose] [submitArg]"
while [[ $# > 0 ]]
do
key="$1"

case $key in
    -h|--help)
    echo "$usage"
    exit 0
    ;;
    -n|--dry-run)
    DRYRUN="-n"
    ;;
    -v|--verbose)
    VERBOSE=1
    ;;
    *)
    submitArg="$*"
    break
    ;;
esac
shift # past argument or value
done

echo "nothing" | $SUBMIT -c $CONF $DRYRUN $submitArg 2>>$LOGERR
