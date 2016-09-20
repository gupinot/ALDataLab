#!/usr/bin/env bash

exec 1>>/root/zeppelin/logs/zeppelin-restart.log
exec 2>&1

echo ""
echo "$(date +"%Y-%m-%dT%H:%M") : begin"

#restart zeppelin
/root/zeppelin/bin/zeppelin-daemon.sh restart

#run all notebook which name contains "prod - "
while ! curl http://localhost:8080/api/notebook 1>/dev/null 2>&1
do
    sleep 5
done

curl http://localhost:8080/api/notebook 2>/dev/null\
| jq -c '.body | sort_by(.name) | map(select(.name | contains("- prod"))) | .[] | .id'\
| tr -d '"' | xargs -I@ curl -d '' http://localhost:8080/api/notebook/job/@

echo "$(date +"%Y-%m-%dT%H:%M") : end"
echo "-------------------------------"
