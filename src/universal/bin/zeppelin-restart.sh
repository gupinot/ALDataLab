#!/usr/bin/env bash

exec 1>>/root/log/zeppelin-restart.log
exec 2>&1
echo ""
echo "$(date +"%Y-%m-%dT%H:%M") : begin"

#restart zeppelin
/root/zeppelin/bin/zeppelin-daemon.sh restart

#run all notebook which name contains "prod - "
sleep 15
curl http://localhost:8080/api/notebook \
| jq -c '.body | sort_by(.name) | map(select(.name | contains("- prod"))) | .[] | .id'\
| tr -d '"' | xargs -I@ curl -d '' http://localhost:8080/api/notebook/job/@

echo "$(date +"%Y-%m-%dT%H:%M") : end"
echo ""