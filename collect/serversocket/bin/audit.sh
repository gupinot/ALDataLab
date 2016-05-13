#!/usr/bin/env bash

CONF=$(dirname $0)/../conf/conf.sh
. $CONF

#Audit :
# Input
# - last version of AIP Server
# - servers list to implement
# - servers list implemented by csc
#	- detected ko by csc : csc was not be able to implement (no passwd for ex)
#	- deployed ok by CSC (3rd column equal to "done")
# - servers list status
#	0 : deployed ok by csc but test is ko
#	1 : deployed ok by csc and test is ok
#	2 : collect deployed
#	10 : deployed ok by csc but cannot test (time-out on connection : server behind a fw)
#
# Output
# - servers deployed ok by csc but not tested (not in servers list status)
#	- servers unknown (not in "servers list to implement" and not in "last version of AIP Server")
# - servers not in scope but are Linux or Unix, are application server type and are active
# - servers list status update :
#	0 : test result different from ko
#	1 : test result ko
#	2 : test result ko
#	10 : test result different from with time-out connection


DATECUR=$(date --utc --date "now" +"%Y%m%d-%H%M%S")

