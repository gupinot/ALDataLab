#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

function synchro() {
	#synchronize server to have monitoring log at same timestamp
	SERVERIP=$1
	RET_SYNCHRO=1
	TIMEDELTA=""
	TIMEDELTA=$((($(perl -e 'print time, "\n"') - $(ssh -o ConnectTimeout=10 -o "BatchMode=yes" datalab@$SERVERIP "perl -e 'print time, \"\n\"'"))/60))
	ssh -o ConnectTimeout=10 -o "BatchMode=yes" datalab@$SERVERIP "echo TIMEDELTA=\\\"$TIMEDELTA\\\" > timedelta.sh" &&\
	RET_SYNCHRO=0
	return $RET_SYNCHRO
}

function deploy() {
	#deploy server script on datalab@$SERVER:/var/tmp/script and create corresponding crontab
	SERVERIP=$1
	HOST=$2
	OSTYPE=$3
	[[ "$HOST" == "" ]] && HOST=$SERVERIP
	RET_DEPLOY=1
	test $SERVERIP $HOST $OSTYPE &&\
	 TMP_SCRIPT=$(mktemp) &&\
	 cat $SCRIPT_SERVER | sed -e "s/^HOSTNAME=\".*\"$/HOSTNAME=\"${HOST}\"/" | sed -e "s/OS_TYPE/${OSTYPE}/" > $TMP_SCRIPT &&\
	 scp $TMP_SCRIPT datalab@$SERVERIP:~/$(basename $SCRIPT_SERVER) &&\
	 rm $TMP_SCRIPT &&\
	 ssh -o ConnectTimeout=10  -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "chmod +x ~/$(basename $SCRIPT_SERVER) && echo \"*/5 * * * * ~/$(basename $SCRIPT_SERVER) monitor 2>~/monitor.err 1>~/monitor.out\" >> mycron && crontab mycron" &&\
	 RET_DEPLOY=0
	return $RET_DEPLOY
}

function update() {
	#update server script on datalab@$SERVERIP:/var/tmp/script and create corresponding crontab
	SERVERIP=$1
	HOST=$2
	[[ "$HOST" == "" ]] && HOST=$SERVERIP
	OSTYPE=$3
	RET_UPDATE=1

	test $SERVERIP $HOST $OSTYPE &&\
	 TMP_SCRIPT=$(mktemp) &&\
	 cat $SCRIPT_SERVER | sed -e "s/^HOSTNAME=\".*\"$/HOSTNAME=\"${HOST}\"/" | sed -e "s/OS_TYPE/${OSTYPE}/" > $TMP_SCRIPT &&\
	 scp $TMP_SCRIPT datalab@$SERVERIP:~/$(basename $SCRIPT_SERVER) &&\
	 rm $TMP_SCRIPT &&\
	 RET_UPDATE=0
	return $RET_UPDATE
}

function undeploy() {
	SERVERIP=$1
	RET_DEPLOY=1
	test $SERVERIP &&\
	 ssh -o ConnectTimeout=10  -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "crontab -r; rm -fr monitor collect monitor.sh monitor.out monitor.err timedelta.sh" &&\
	 RET_DEPLOY=0
	return $RET_DEPLOY
}

function collect() {
	SERVERIP=$1
	HOST=$2
	[[ "$HOST" == "" ]] && HOST=$SERVERIP
	DATECUR=$(date --utc --date "now" +"%Y%m%d-%H%M%S")
	RET_COLLECT=1
	#collect data of server
	ssh -o ConnectTimeout=10  -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "~/$(basename $SCRIPT_SERVER) collect" &&\
	 scp datalab@$SERVERIP:~/collect/*.gz $DIR_COLLECT/. &&\
	 ssh -o ConnectTimeout=10  -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "rm -f ~/collect/*.gz" &&\
	 RET_COLLECT=0
	echo "$HOST;$SERVERIP;$RET_COLLECT;$DATECUR" >> $SERVERCOLLECT
	echo "$HOST;$SERVERIP;$RET_COLLECT;$DATECUR" 
	return $RET_COLLECT
}

function test() {
	SERVERIP=$1
	HOST=$2
	[[ "$HOST" == "" ]] && HOST=$SERVERIP
	OSTYPE=$3
	SSH_RET=1
	SSH_ERR=""
	HOME_DIR=""
	NETSTAT_RET=1
	NETSTAT_ERR=""
	LSOF_RET=1
	LSOF_ERR=""
	CRONTAB_RET=1
	CRONTAB_ERR=""

	errlog=$(mktemp)
	stdoutlog=$(mktemp)

	#ssh with key
	[[ "$DEBUG" == "YES" ]] && echo "test ssh"
	ssh -o ConnectTimeout=10 -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "pwd" 1>$stdoutlog 2>$errlog && SSH_RET=0
	[[ $SSH_RET -ne 0 ]] && SSH_ERR="$(head -n 1 $errlog | tr -d '\n' | tr -d '\r')"

	#home directory
	[[ $SSH_RET -eq 0 ]] && grep -q datalab $stdoutlog && HOME_DIR="$(head -n 1 $stdoutlog)"

	#sudo on netstat
	[[ "$DEBUG" == "YES" ]] && echo "test netstat"
	[[ $SSH_RET -eq 0 ]] &&\
	(
		rm -f $errlog
		rm -f $stdoutlog
		if [[ "$OSTYPE" == "linux" ]]
		then
			ssh -o ConnectTimeout=10 -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "sudo -n netstat -anp || yes | sudo netstat -anp" 1>$stdoutlog 2>$errlog && NETSTAT_RET=0
		else
			ssh -o ConnectTimeout=10 -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "PATH=\"/usr/local/bin:/opt/sfw/bin:/opt/csw/bin:$PATH\"; yes | sudo netstat -an" 1>$stdoutlog 2>$errlog && NETSTAT_RET=0
		fi
		return $NETSTAT_RET
	) && NETSTAT_RET=$?
	[[ $SSH_RET -eq 0 ]] && [[ $NETSTAT_RET -ne 0 ]] && NETSTAT_ERR="$(cat $errlog | grep -v "Invalid argument" | grep -v "Connection to .* closed." | grep -v "using fake authentication data for X11 forwarding"| grep -vi "sudo: illegal option" | head -n 1 | tr -d '\n' | tr -d '\r')" && NETSTAT_ERR="$NETSTAT_ERR | $(head -n 1 $stdoutlog |  tr -d '\n' | tr -d '\r')"

	#sudo on lsof
	[[ "$DEBUG" == "YES" ]] && echo "test lsof"
	[[ $SSH_RET -eq 0 ]] &&\
	(
		rm -f $errlog
		rm -f $stdoutlog
		if ssh -o ConnectTimeout=10 -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "bash -c \"[[ -f /usr/sbin/lsof ]] ||[[ -f /usr/bin/lsof ]] ||[[ -f /usr/local/bin/lsof ]]\""
		then
			if [[ "$OSTYPE" == "linux" ]]
			then
				ssh -o ConnectTimeout=10 -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "sudo -n /usr/sbin/lsof -nP -i || sudo -n /usr/bin/lsof -nP -i || yes | sudo /usr/sbin/lsof -nP -i || yes | sudo /usr/bin/lsof -nP -i" 1>$stdoutlog 2>$errlog && LSOF_RET=0
			else
				ssh -o ConnectTimeout=10 -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "PATH=\"/usr/local/bin:/opt/sfw/bin:/opt/csw/bin:$PATH\"; yes | sudo /usr/sbin/lsof -nP -i || yes | sudo /usr/bin/lsof -nP -i || yes | sudo /usr/local/bin/lsof -nP -i" 1>$stdoutlog 2>$errlog && LSOF_RET=0
			fi
		else
			echo "no lsof found" > $errlog
			echo "" > $stdoutlog
		fi
		return $LSOF_RET
	) && LSOF_RET=$?
	[[ $SSH_RET -eq 0 ]] && [[ $LSOF_RET -ne 0 ]] && LSOF_ERR="$(cat $errlog | grep -v "Invalid argument" | grep -v "Connection to .* closed." | grep -v "using fake authentication data for X11 forwarding" | grep -vi "sudo: illegal option" | head -n 1 | tr -d '\n' | tr -d '\r')" && LSOF_ERR="$LSOF_ERR | $(head -n 1 $stdoutlog |  tr -d '\n' | tr -d '\r')"

	PFILES_RET=1
	PFILES_ERR=""
	#test sudo pfiles /proc/* for unix os if lsof not working 
	[[ $SSH_RET -eq 0 ]] && [[ $LSOF_RET -ne 0 ]] && [[ "$OSTYPE" != "linux" ]]  &&\
	(
		rm -f $errlog
		rm -f $stdoutlog
		ssh -o ConnectTimeout=10 -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "sudo pfiles /proc/*" 1>$stdoutlog 2>$errlog && PFILES_RET=0
		return $PFILES_RET
	) && PFILES_RET=$? &&\
	    if [[ $PFILES_RET -ne 0 ]]; then
	        PFILES_ERR="$(cat $errlog | grep -v "Invalid argument" | grep -v "Connection to .* closed." | grep -v "using fake authentication data for X11 forwarding" | grep -vi "sudo: illegal option" | head -n 1 | tr -d '\n' | tr -d '\r')" &&\
	        PFILES_ERR="$PFILES_ERR | $(head -n 1 $stdoutlog |  tr -d '\n' | tr -d '\r')"
	        LSOF_ERR="$LSOF_ERR | $PFILES_ERR"
	    else
	        LSOF_RET=0
	        LSOF_ERR="sudo lsof ko but sudo pfiles ok"
	    fi



	#crontab
	[[ "$DEBUG" == "YES" ]] && echo "test crontab"
	[[ $SSH_RET -eq 0 ]] &&\
	(
		rm -f $errlog
		rm -f $stdoutlog
		ssh -o ConnectTimeout=10 -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVERIP "echo '00 09 * * 1-5 echo hello' >> mycron && (crontab -l > crontab_save 2>/dev/null || echo "nothing" 1>/dev/null) && crontab mycron && crontab -l && rm mycron && crontab -r && (crontab crontab_save 2>/dev/null || echo "nothing" 1>/dev/null)" 1>$stdoutlog 2>$errlog &&\
		([[ ! -f $errlog ]] || [[ $(cat $errlog |grep -v "warning" | grep -v "using fake authentication data for X11 forwarding" | wc -l | awk '{print $1}') -eq 0 ]]) && CRONTAB_RET=0
		return $CRONTAB_RET
	) && CRONTAB_RET=$?
	[[ $SSH_RET -eq 0 ]] && [[ $CRONTAB_RET -ne 0 ]] && CRONTAB_ERR="$(cat $errlog | grep -v "using fake authentication data for X11 forwarding" | head -n 1 | tr -d '\n' | tr -d '\r')"

	echo "\"$(date +"%Y-%m-%dT%H:%M:%S.000Z")\";\"$HOST\";\"$SERVERIP\";\"${SSH_RET}\";\"${SSH_ERR}\";\"${HOME_DIR}\";\"$NETSTAT_RET\";\"${NETSTAT_ERR}\";\"$LSOF_RET\";\"${LSOF_ERR}\";\"${CRONTAB_RET}\";\"${CRONTAB_ERR}\""

	TEST_RET=1
	[[ $SSH_RET -eq 0 ]] && [[ $NETSTAT_RET -eq 0 ]] && [[ $LSOF_RET -eq 0 ]] && [[ $CRONTAB_RET -eq 0 ]] && TEST_RET=0
	return $TEST_RET
}

########################################################################################################################
#main

usage="$0 test|deploy|collect|undeploy|synchro ipserver hostname"

ServerType="linux"
host="host"
while [[ $# > 0 ]]
do
   key="$1"

   case ${key} in
     -h|--help)
        echo ${usage}
        exit 0
        ;;
     test|deploy|update|collect|undeploy|synchro)
        method=$key
        serverIP=$2
        host=$3
        ServerType=$4
        shift
        shift
        shift
        ;;
    esac
    shift # past argument or value
done

ServerType=$(echo $ServerType | tr '[:upper:]' '[:lower:]')

case $method in
  test)
    test $serverIP $host $ServerType
    ;;
  deploy)
    deploy $serverIP $host $ServerType
    synchro $serverIP $host $ServerType
    ;;
  update)
    update $serverIP $host $ServerType
    synchro $serverIP $host $ServerType
    ;;
  undeploy)
    undeploy $serverIP $host $ServerType
    ;;
  collect)
    collect $serverIP $host $ServerType
    synchro $serverIP $host $ServerType
    ;;
  synchro)
    synchro $serverIP $host $ServerType
    ;;
esac
exit $?
