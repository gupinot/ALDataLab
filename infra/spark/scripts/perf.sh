yum --enablerepo='*-debug*' install -q -y java-1.8.0-openjdk-debuginfo.x86_64
# Perf tools
yum install -y dstat iotop strace sysstat htop perf
debuginfo-install -q -y glibc
debuginfo-install -q -y kernel
