#!/usr/bin/env bash
# PySpark and MLlib deps
yum install -y  python-matplotlib python-tornado scipy libgfortran
# SparkR deps
yum install -y R

# Create /usr/bin/realpath which is used by R to find Java installations
# NOTE: /usr/bin/realpath is missing in CentOS AMIs. See
# http://superuser.com/questions/771104/usr-bin-realpath-not-found-in-centos-6-5
cat >/usr/bin/realpath <<EOF
#!/bin/bash

readlink -e "$@"
EOF

chmod a+x /usr/bin/realpath

# Ganglia
yum install -y ganglia ganglia-web ganglia-gmond ganglia-gmetad