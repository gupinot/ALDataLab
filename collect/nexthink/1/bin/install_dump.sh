#!/bin/bash
HOSTNAME="$(uname -n)"

function xmlfieldvalue {
        field=$1
        input=$2
        echo $input | sed "s/ //g" | sed "s/<$field>//g" | sed "s/<\/$field>//g"
        return 0
}

rootNexthinkDir=/var/nexthink/engine/01
#rootNexthinkDir=$HOME/simuProd
tmpFile=$HOME/tmpFile

dumpFile=$1
if [ ! -f $dumpFile ]
then
        echo "|$HOSTNAME|$0|dumpFile $dumpFile not found"
        exit 1
fi

echo "$(date +"%Y/%m/%d-%H:%M:%S")|$HOSTNAME|$0|start of $dumpFile compute"

nxengineXML=$rootNexthinkDir/etc/nxengine.xml
nxengineDB=$rootNexthinkDir/data/nxengine.db
sudo cp $nxengineXML.sav $nxengineXML

echo "$(date +"%Y/%m/%d-%H:%M:%S") |$HOSTNAME|$0| stop engine"
nxinfo launch -s

function detarAndCopyDB {
	ret_detarAndCopyDB=1
        cd $HOME &&\
        rm -fr temp &&\
        mkdir temp &&\
        cd temp &&\
        mv $dumpFile . &&\
        tar xvfz *.tgz &&\
        cd data &&\
        gunzip nxengine-db.gz &&\
        sudo cp nxengine* $nxengineDB &&\
        sudo chown root:root $nxengineDB &&\
        sudo chmod 644 $nxengineDB &&\
	ret_detarAndCopyDB=0
	return $ret_detarAndCopyDB
}

echo "$(date +"%Y/%m/%d-%H:%M:%S") |$HOSTNAME|$0| detarAndCopyDB... "
detarAndCopyDB; ret=$?
if [[ $ret -ne 0 ]]
then
	echo "$(date +"%Y/%m/%d-%H:%M:%S") |$HOSTNAME|$0| detarAndCopyDB : ERR"
	exit 1
fi
echo "$(date +"%Y/%m/%d-%H:%M:%S") |$HOSTNAME|$0| detarAndCopyDB end "


cd $HOME/temp/etc
NewEngineName=$(xmlfieldvalue "server" $(grep "<server>.*</server>" nxengine.xml | head -1))
OldEngineName=$(xmlfieldvalue "server" $(grep "<server>.*</server>" $nxengineXML | head -1))
echo "$(date +"%Y/%m/%d-%H:%M:%S") |$HOSTNAME|$0| NewEngineName=$NewEngineName; OldEngineName=$OldEngineName"
if [[ "$NewEngineName" == "" ]]
then
	echo "$(date +"%Y/%m/%d-%H:%M:%S") |$HOSTNAME|$0| NewEngineName empty : ERR"
	exit 1
fi
sed "s/$OldEngineName/$NewEngineName/g" $nxengineXML > $tmpFile
sudo mv $tmpFile $nxengineXML
sudo chown root:root $nxengineXML
sudo chmod 644 $nxengineXML

cd $HOME/temp/data
lastValue=$(nxinfo database -f nxengine-db | grep end | awk -F' ' '{print $2}')
#lastValue=$(date +"%Y-%m-%dT%H:%M:%S")
staticNow=$(xmlfieldvalue "static_now" $(grep "static_now" $nxengineXML))
if [[ "$staticNow" != "" ]]
then
        sed "s/$staticNow/$lastValue/g" $nxengineXML > $tmpFile
        sudo mv $tmpFile $nxengineXML
	sudo chown root:root $nxengineXML
	sudo chmod 644 $nxengineXML
fi

#start engine
echo "$(date +"%Y/%m/%d-%H:%M:%S") |$HOSTNAME|$0| start engine..."
sudo /etc/init.d/nxlaunch start

function wait_running {
        trouve=0
        while [ $trouve -ne 1 ]
        do
                sleep 10
                trouve=$(nxinfo launch -l | grep running | wc -l)
        done
        trouve=0
        while [ $trouve -lt 10 ]
        do
                sleep 10
                trouve=$(nxinfo info | wc -l)
        done
}
echo "$(date +"%Y/%m/%d-%H:%M:%S") |$HOSTNAME|$0| wait running..."
wait_running
echo "$(date +"%Y/%m/%d-%H:%M:%S") |$HOSTNAME|$0| running ok"

echo "$(date +"%Y/%m/%d-%H:%M:%S") |$HOSTNAME|$0| reset admin passwd"
nxinfo shell -e "update login.password=\"admin\" where login.name=\"admin\""

echo "$(date +"%Y/%m/%d-%H:%M:%S") |$HOSTNAME|$0| End of $dumpFile compute"

