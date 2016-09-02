#!/bin/bash

repo=${1:-s3repo}
command=/etc/elasticsearch/commands

if ! curl -sf "http://localhost:9200/_snapshot/$repo" > /dev/null
then
   echo -n "Creating repo $repo..."
   if curl -sf -XPUT http://localhost:9200/_snapshot/$repo --data-binary @$command/$repo.json >/dev/null
   then
      echo "success"
      exit 0
   else
      echo "failed"
      exit 1
   fi
fi
