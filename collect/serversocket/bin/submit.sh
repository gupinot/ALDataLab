#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

function deploy() {
	#deploy server script on datalab@$SERVER:/var/tmp/script and create corresponding crontab
	SERVER=$1
	RET_DEPLOY=1
	test $SERVER &&\
	 TMP_SCRIPT=$(mktemp) &&\
	 cat $SCRIPT_SERVER | sed -e "s/HOST_NAME/$SERVER/" > $TMP_SCRIPT &&\
	 scp $TMP_SCRIPT datalab@$SERVER:~/$(basename $SCRIPT_SERVER) &&\
	 rm $TMP_SCRIPT &&\
	 ssh  -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVER "chmod +x ~/$(basename $SCRIPT_SERVER) && echo \"*/5 * * * * ~/$(basename $SCRIPT_SERVER) monitor\" >> mycron && crontab mycron" &&\
	 RET_DEPLOY=0
	return $RET_DEPLOY
}

function collect() {
	SERVER=$1
	RET_COLLECT=1
	#collect data of server
	ssh  -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVER "~/$(basename $SCRIPT_SERVER) collect" &&\
	 scp datalab@$SERVER:~/collect/*.gz $DIR_COLLECT/. &&\
	 ssh  -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVER "rm -f ~/collect/*.gz" &&\
	 RET_COLLECT=0
	return $RET_COLLECT
}

function test() {
	SERVER=$1
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
	ssh -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVER "pwd" 1>$stdoutlog 2>$errlog && SSH_RET=0
	[[ $SSH_RET -ne 0 ]] && SSH_ERR="$(head -n 1 $errlog | tr -d '\n' | tr -d '\r')"

	#home directory
	[[ $SSH_RET -eq 0 ]] && grep -q datalab $stdoutlog && HOME_DIR="$(head -n 1 $stdoutlog)"

	#sudo on netstat
	[[ $SSH_RET -eq 0 ]] &&\
	(
		rm -f $errlog
		rm -f $stdoutlog
		ssh -o "BatchMode=yes" -o StrictHostKeyChecking=no -t -t datalab@$SERVER "sudo -n netstat -anp || yes | sudo netstat -anp" 1>$stdoutlog 2>$errlog && NETSTAT_RET=0
		return $NETSTAT_RET
	) && NETSTAT_RET=$?
	[[ $SSH_RET -eq 0 ]] && [[ $NETSTAT_RET -ne 0 ]] && NETSTAT_ERR="$(cat $errlog | grep -v "Invalid argument" | grep -v "Connection to .* closed." | grep -v "using fake authentication data for X11 forwarding"| head -n 1 | tr -d '\n' | tr -d '\r')" && NETSTAT_ERR="$NETSTAT_ERR | $(head -n 1 $stdoutlog |  tr -d '\n' | tr -d '\r')"

	#sudo on lsof
	[[ $SSH_RET -eq 0 ]] &&\
	(
		rm -f $errlog
		rm -f $stdoutlog
		ssh -o "BatchMode=yes" -o StrictHostKeyChecking=no -t -t datalab@$SERVER "sudo -n /usr/sbin/lsof -nP -i || sudo -n /usr/bin/lsof -nP -i || yes | sudo /usr/sbin/lsof -nP -i || yes | sudo /usr/bin/lsof -nP -i" 1>$stdoutlog 2>$errlog && LSOF_RET=0
		return $LSOF_RET
	) && LSOF_RET=$?
	[[ $SSH_RET -eq 0 ]] && [[ $LSOF_RET -ne 0 ]] && LSOF_ERR="$(cat $errlog | grep -v "Invalid argument" | grep -v "Connection to .* closed." | grep -v "using fake authentication data for X11 forwarding" | head -n 1 | tr -d '\n' | tr -d '\r')" && LSOF_ERR="$LSOF_ERR | $(head -n 1 $stdoutlog |  tr -d '\n' | tr -d '\r')"
	

	#crontab
	[[ $SSH_RET -eq 0 ]] &&\
	(
		rm -f $errlog
		rm -f $stdoutlog
		ssh -o "BatchMode=yes" -o StrictHostKeyChecking=no datalab@$SERVER "echo '00 09 * * 1-5 echo hello' >> mycron && crontab mycron && crontab -l && rm mycron && crontab -r" 1>$stdoutlog 2>$errlog &&\
		([[ ! -f $errlog ]] || [[ $(cat $errlog | grep -v "using fake authentication data for X11 forwarding" | wc -l | awk '{print $1}') -eq 0 ]]) && CRONTAB_RET=0
		return $CRONTAB_RET
	) && CRONTAB_RET=$?
	[[ $SSH_RET -eq 0 ]] && [[ $CRONTAB_RET -ne 0 ]] && CRONTAB_ERR="$(cat $errlog | grep -v "using fake authentication data for X11 forwarding" | head -n 1 | tr -d '\n' | tr -d '\r')"

	echo "$SERVER;${SSH_RET};${SSH_ERR};${HOME_DIR};$NETSTAT_RET;${NETSTAT_ERR};$LSOF_RET;${LSOF_ERR};${CRONTAB_RET};${CRONTAB_ERR}"
	
	TEST_RET=1
	[[ $SSH_RET -eq 0 ]] && [[ $NETSTAT_RET -eq 0 ]] && [[ $LSOF_RET -eq 0 ]] && [[ $CRONTAB_RET -eq 0 ]] && TEST_RET=0
	return $TEST_RET
}

########################################################################################################################
#main

usage="$0 test|deploy|collect server"

while [[ $# > 0 ]]
do
   key="$1"

   case ${key} in
     -h|--help)
        echo ${usage}
        exit 0
        ;;
     test|deploy|collect)
	method=$key
	server=$2
	shift
        ;;
    esac
    shift # past argument or value
done

case $method in
  test)
    test $server
    ;;
  deploy)
    deploy $server
    ;;
  collect)
    collect $server
    ;;
esac
exit $?
