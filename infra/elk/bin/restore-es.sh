#!/bin/bash

repo=${1:-s3repo}

/usr/local/bin/create-repo.sh $repo

SNAPSHOTNAME=$(curator --loglevel WARN show snapshots --repository $repo | sort -r | head -1)

echo "Restoring $SNAPSHOTNAME..."

curl -XPOST http://localhost:9200/_snapshot/$repo/$SNAPSHOTNAME/_restore
