#!/bin/bash

repo=${1:-s3repo}
/usr/local/bin/create-repo.sh $repo
curator snapshot --repository $repo indices --all-indices
