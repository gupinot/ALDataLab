#!/bin/bash -eux

set -e

# Install base packages
yum -y update
yum -y install vim curl wget unzip screen jq python-pip git rsync pssh

# Root ssh config
sed -i 's/PermitRootLogin.*/PermitRootLogin without-password/g' /etc/ssh/sshd_config
sed -i 's/^.*AuthorizedKeysFile/AuthorizedKeysFile .ssh\/authorized_keys2 /' /etc/ssh/sshd_config
sed -i 's/disable_root.*/disable_root: 0/g' /etc/cloud/cloud.cfg

# Set up ephemeral mounts
sed -i 's/mounts.*//g' /etc/cloud/cloud.cfg
sed -i 's/.*ephemeral.*//g' /etc/cloud/cloud.cfg
sed -i 's/.*swap.*//g' /etc/cloud/cloud.cfg

echo "mounts:" >> /etc/cloud/cloud.cfg

for x in {0..23}; do
    if [[ $x -eq 0 ]]; then
        mnt="/mnt"
    else
        mnt="/mnt$((x+1))"
    fi
    echo " - [ ephemeral$x, $mnt, auto, \"defaults,noatime,nodiratime\", \"0\", \"0\" ]" >> /etc/cloud/cloud.cfg
done

# Setup default keys
cat >/root/.ssh/authorized_keys2 <<EOF
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCX1ZB+xPsgEcb3CAUg7lp28ilFijn7xSeh+F2G61CgbgP+S6BjBAJSVG+Zl+vzHYKV4hoWRp3y6wBzotEzejcy8aAIxT6aAMfYSpJKBc29ZLNbtxyCc6+ndP+wZyr4JnNMVaLFBZUWuF4U9Ml2kYWJyMbokk5PhB3fx46PAm6+DyP31EWHDZBpTJjhm2dW8Qo0QftWb/38bQKjtXuzdVuYDUVvpdubYCJR0o8Aa0nOSH2BBLocVKjP9DfIn5w8oCie8E5BE0QPjHH9xLoXbLSpiPy+RjsHeTli6ACZ6iPhUIVOJK6eJ6sjV/cem5cXb1hkf5BH0+FXQJse8EexSfSH force-rluta
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCI2RMMz9MAYJrUuqo64qw7DVNQ8q6Si2GH/4NafdRlFkePy5L0SmPveV5R0N7vaOPK+bdiJF2MG/piRhPnhAlsOQ4+JFfZTNEC+U75ee/3rfsG/r8DiLbjF6yvCSr0fYixVkO2ST4sxZMSoKwa9ZoqMVdDwUmOTGylbnOklrpp95punGxhjzocBqKib5F3XPfmOgxRYR2FuRvSQgCGQXz5BqBCz0DrGmK5CI6Md5xnokg6aOacwe2y4NlMrCoo58c+GVX6xXWjeF/g/1MC0kiNfBxoImviOl8L7ajYMKF3MfrHdWHRRvUtWz3X4qbnOJdJra0inEV2TBC1jzC7Q5wf force-KeyLezoomer
EOF

chmod 600 /root/.ssh/authorized_keys2
