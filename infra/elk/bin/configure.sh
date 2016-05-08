#!/bin/bash

source=$1
target=$2

mv -f $target $target.orig
cat $source | perl -pe 's/\$\{(.+)\}/$ENV{$1}/e' >$target
