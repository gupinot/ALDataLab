#!/bin/bash
Dir=$(dirname $0)
cd $Dir

./mainV2.sh 5&
./mainV2.sh 6&

