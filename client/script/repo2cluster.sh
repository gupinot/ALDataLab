#!/usr/bin/env sh

#Send new files from local repo dir to s3
LOCAL_DIR=$1
DISTANT_DIR=$2
TODOFILE=/tmp/todofile.todo.$$
echo "todo" > $TODOFILE

for localfile in $(ls -l $LOCAL_DIR | awk '{print $5";"$9}')
do
    localfilefullpath=$(echo $localfile | awk -F';' '{print $2}')
    localfilename=$(basename $localfilefullpath)
    localfilesize=$(echo $localfile | awk -F';' '{print $1}')

    distantfile=$(aws s3 ls $DISTANT_DIR/${localfilename} | grep -v ".todo" | awk '{print $3";"$4}')
    distantfilename=$(echo $distantfile | awk -F';' '{print $2}')
    distantfilesize=$(echo $distantfile | awk -F';' '{print $1}')

    echo "$localfilesize;$localfilename    $distantfile"

    if [[ ${distantfilesize} -ne ${localfilesize} ]]
    then
        echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : aws s3 cp $localfilefullpath $DISTANT_DIR/$localfilename..."
        aws s3 cp $localfilefullpath $DISTANT_DIR/$localfilename
        ret=$?
        if [[ $ret -eq 0 ]]
        then
            aws s3 cp $TODOFILE $DISTANT_DIR/$localfilename.todo
            ret=$?
            if [[ $ret -eq 0 ]]
            then
                echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : aws s3 cp $localfilefullpath $DISTANT_DIR/$localfilename : Done"
                mv $localfilefullpath $(dirname $localfilefullpath)/Archives/.
            fi
        else
            echo "$(date +"%Y/%m/%d-%H:%M:%S") - $0 : aws s3 cp $localfilefullpath $DISTANT_DIR/$localfilename : ERR"
        fi
    fi
done
rm $TODOFILE