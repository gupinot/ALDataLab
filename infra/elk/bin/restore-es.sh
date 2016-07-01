#!/bin/bash

SNAPSHOTNAME=$(curator --loglevel WARN show snapshots --repository s3repo | sort -r | head -1)

echo "Restoring $SNAPSHOTNAME..."

curl -XPOST http://localhost:9200/_snapshot/s3repo/$SNAPSHOTNAME/_restore