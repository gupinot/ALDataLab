#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import division, print_function, with_statement

import codecs
import hashlib
import itertools
import logging
import os
import os.path
import pipes
import random
import shutil
import string
from stat import S_IRUSR
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import time
import warnings
from datetime import datetime
from optparse import OptionParser
from sys import stderr

if sys.version < "3":
    from urllib2 import urlopen, Request, HTTPError
else:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
    raw_input = input
    xrange = range

SPARK_EC2_VERSION = "1.6.2"
SPARK_EC2_DIR = os.path.dirname(os.path.realpath(__file__))

VALID_SPARK_VERSIONS = set([
    "1.6.2",
    "2.0.0"
])

SPARK_ZEPPELIN_MAP = {
    "1.6.2": "0.6.1",
    "2.0.0": "0.6.1"
}

SPARK_HADOOP_MAP = {
    "1.6.2": "2.4",
    "2.0.0": "2.7"
}

SPARK_SCALA_MAP = {
    "1.6.2": "2.10",
    "2.0.0": "2.11"
}

SPARK_TACHYON_MAP = {
    "1.6.2": "0.8.2",
    "2.0.0": "1.2.0"
}

# Source: http://aws.amazon.com/amazon-linux-ami/instance-type-matrix/
# Last Updated: 2015-06-19
# For easy maintainability, please keep this manually-inputted dictionary sorted by key.
EC2_INSTANCE_TYPES = {
    "c1.medium":   "pvm",
    "c1.xlarge":   "pvm",
    "c3.large":    "pvm",
    "c3.xlarge":   "pvm",
    "c3.2xlarge":  "pvm",
    "c3.4xlarge":  "pvm",
    "c3.8xlarge":  "pvm",
    "c4.large":    "hvm",
    "c4.xlarge":   "hvm",
    "c4.2xlarge":  "hvm",
    "c4.4xlarge":  "hvm",
    "c4.8xlarge":  "hvm",
    "cc1.4xlarge": "hvm",
    "cc2.8xlarge": "hvm",
    "cg1.4xlarge": "hvm",
    "cr1.8xlarge": "hvm",
    "d2.xlarge":   "hvm",
    "d2.2xlarge":  "hvm",
    "d2.4xlarge":  "hvm",
    "d2.8xlarge":  "hvm",
    "g2.2xlarge":  "hvm",
    "g2.8xlarge":  "hvm",
    "hi1.4xlarge": "pvm",
    "hs1.8xlarge": "pvm",
    "i2.xlarge":   "hvm",
    "i2.2xlarge":  "hvm",
    "i2.4xlarge":  "hvm",
    "i2.8xlarge":  "hvm",
    "m1.small":    "pvm",
    "m1.medium":   "pvm",
    "m1.large":    "pvm",
    "m1.xlarge":   "pvm",
    "m2.xlarge":   "pvm",
    "m2.2xlarge":  "pvm",
    "m2.4xlarge":  "pvm",
    "m3.medium":   "hvm",
    "m3.large":    "hvm",
    "m3.xlarge":   "hvm",
    "m3.2xlarge":  "hvm",
    "m4.large":    "hvm",
    "m4.xlarge":   "hvm",
    "m4.2xlarge":  "hvm",
    "m4.4xlarge":  "hvm",
    "m4.10xlarge": "hvm",
    "r3.large":    "hvm",
    "r3.xlarge":   "hvm",
    "r3.2xlarge":  "hvm",
    "r3.4xlarge":  "hvm",
    "r3.8xlarge":  "hvm",
    "t1.micro":    "pvm",
    "t2.micro":    "hvm",
    "t2.small":    "hvm",
    "t2.medium":   "hvm",
    "t2.large":    "hvm",
}

DEFAULT_SPARK_VERSION = SPARK_EC2_VERSION
DEFAULT_PIPELINE_VERSION = "1.3.2"
DEFAULT_SPARK_GITHUB_REPO = "https://github.com/apache/spark"

# Default location to get the spark-ec2 scripts (and ami-list) from
DEFAULT_SPARK_EC2_GITHUB_REPO = "https://github.com/rluta/spark-ec2"
DEFAULT_SPARK_EC2_BRANCH = "spark-2.0"
DEFAULT_INSTANCE_TYPE="m1.xlarge"

def setup_external_libs(libs):
    """
    Download external libraries from PyPI to SPARK_EC2_DIR/lib/ and prepend them to our PATH.
    """
    PYPI_URL_PREFIX = "https://pypi.python.org/packages/source"
    SPARK_EC2_LIB_DIR = os.path.join(SPARK_EC2_DIR, "lib")

    if not os.path.exists(SPARK_EC2_LIB_DIR):
        print("Downloading external libraries that spark-ec2 needs from PyPI to {path}...".format(
            path=SPARK_EC2_LIB_DIR
        ))
        print("This should be a one-time operation.")
        os.mkdir(SPARK_EC2_LIB_DIR)

    for lib in libs:
        versioned_lib_name = "{n}-{v}".format(n=lib["name"], v=lib["version"])
        lib_dir = os.path.join(SPARK_EC2_LIB_DIR, versioned_lib_name)

        if not os.path.isdir(lib_dir):
            tgz_file_path = os.path.join(SPARK_EC2_LIB_DIR, versioned_lib_name + ".tar.gz")
            print(" - Downloading {lib}...".format(lib=lib["name"]))
            download_stream = urlopen(
                "{prefix}/{first_letter}/{lib_name}/{lib_name}-{lib_version}.tar.gz".format(
                    prefix=PYPI_URL_PREFIX,
                    first_letter=lib["name"][:1],
                    lib_name=lib["name"],
                    lib_version=lib["version"]
                )
            )
            with open(tgz_file_path, "wb") as tgz_file:
                tgz_file.write(download_stream.read())
            with open(tgz_file_path, "rb") as tar:
                if hashlib.md5(tar.read()).hexdigest() != lib["md5"]:
                    print("ERROR: Got wrong md5sum for {lib}.".format(lib=lib["name"]), file=stderr)
                    sys.exit(1)
            tar = tarfile.open(tgz_file_path)
            tar.extractall(path=SPARK_EC2_LIB_DIR)
            tar.close()
            os.remove(tgz_file_path)
            print(" - Finished downloading {lib}.".format(lib=lib["name"]))
        sys.path.insert(1, lib_dir)


# Only PyPI libraries are supported.
external_libs = [
    {
        "name": "boto",
        "version": "2.34.0",
        "md5": "5556223d2d0cc4d06dd4829e671dcecd"
    }
]

setup_external_libs(external_libs)

import boto
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType
from boto import ec2


class UsageError(Exception):
    pass


def parse_args():
    """Configure and parse our command-line arguments"""
    parser = OptionParser(
        prog="spark-ec2",
        version="%prog {v}".format(v=SPARK_EC2_VERSION),
        usage="%prog [options] <action> <cluster_name>\n\n"
              + "<action> can be: launch, destroy, login, stop, start, get-master, reboot-slaves")

    parser.add_option(
        "-s", "--slaves", type="int", default=1,
        help="Number of slaves to launch (default: %default)")
    parser.add_option(
        "-k", "--key-pair",
        help="Key pair to use on instances")
    parser.add_option(
        "-i", "--identity-file",
        help="SSH private key file to use for logging into instances")
    parser.add_option(
        "-p", "--profile", default=None,
        help="If you have multiple profiles (AWS or boto config), you can configure " +
             "additional, named profiles by using this option (default: %default)")
    parser.add_option(
        "-t", "--instance-type", default=DEFAULT_INSTANCE_TYPE,
        help="Type of instance to launch (default: %default). " +
             "WARNING: must be 64-bit; small instances won't work")
    parser.add_option(
        "-m", "--master-instance-type", default=DEFAULT_INSTANCE_TYPE,
        help="Master instance type (leave empty for same as instance-type)")
    parser.add_option(
        "-r", "--region", default="us-east-1",
        help="EC2 region used to launch instances in, or to find them in (default: %default)")
    parser.add_option(
        "-z", "--zone", default="us-east-1a",
        help="Availability zone to launch instances in, or 'all' to spread " +
             "slaves across multiple (an additional $0.01/Gb for bandwidth" +
             "between zones applies) (default: a single zone chosen at random)")
    parser.add_option(
        "-a", "--ami",
        help="Amazon Machine Image ID to use")
    parser.add_option(
        "-v", "--spark-version", default=DEFAULT_SPARK_VERSION,
        help="Version of Spark to use: 'X.Y.Z' or a specific git hash (default: %default)")
    parser.add_option(
        "--scala-version", default=None,
        help="Version of Scala to use: 'X.Y' (default: %default)")
    parser.add_option(
        "--deploy-env", default="prod",
        help="Name of your base deploy directory. Script will look for deploy.<envname> in current directory  (default: %default)")
    parser.add_option(
        "--deploy-profile", default=None,
        help="If you have multiple profiles (AWS or boto config), you can configure " +
             "additional, named profiles by using this option. If not empty, this profile will be deployed"
             "to the master instead of profile option  (default: %default)")
    parser.add_option(
        "--deploy-aws-key-id", default=None,
        help="AWS key id to deploy to server if copy credentials is enabled  (default: %default)")
    parser.add_option(
        "--deploy-aws-key-secret", default=None,
        help="AWS key secret to deploy to server if copy credentials is enabled  (default: %default)")
    parser.add_option(
        "--spark-git-repo",
        default=DEFAULT_SPARK_GITHUB_REPO,
        help="Github repo from which to checkout supplied commit hash (default: %default)")
    parser.add_option(
        "--spark-ec2-git-repo",
        default=DEFAULT_SPARK_EC2_GITHUB_REPO,
        help="Github repo from which to checkout spark-ec2 (default: %default)")
    parser.add_option(
        "--spark-ec2-git-branch",
        default=DEFAULT_SPARK_EC2_BRANCH,
        help="Github repo branch of spark-ec2 to use (default: %default)")
    parser.add_option(
        "--deploy-root-dir",
        default=SPARK_EC2_DIR+"/deploy.master/",
        help="A directory to copy into / on the first master. " +
             "Must be absolute. Note that a trailing slash is handled as per rsync: " +
             "If you omit it, the last directory of the --deploy-root-dir path will be created " +
             "in / before copying its contents. If you append the trailing slash, " +
             "the directory is not created and its contents are copied directly into /. " +
             "(default: %default).")
    parser.add_option(
        "--yarn", action="store_true", default=True,
        help="Enable YARN support in spark and start YARN daemon in Hadoop (default: %default)")
    parser.add_option(
        "--tachyon", action="store_true", default=False,
        help="Enable Tachyon support in spark and start Tachyon daemon in Hadoop (default: %default)")
    parser.add_option(
        "-D", metavar="[ADDRESS:]PORT", dest="proxy_port",
        help="Use SSH dynamic port forwarding to create a SOCKS proxy at " +
             "the given local address (for use with login)")
    parser.add_option(
        "--resume", action="store_true", default=False,
        help="Resume installation on a previously launched cluster " +
             "(for debugging)")
    parser.add_option(
        "--ebs-vol-size", metavar="SIZE", type="int", default=0,
        help="Size (in GB) of each EBS volume.")
    parser.add_option(
        "--ebs-vol-type", default="standard",
        help="EBS volume type (e.g. 'gp2', 'standard').")
    parser.add_option(
        "--ebs-vol-num", type="int", default=1,
        help="Number of EBS volumes to attach to each node as /vol[x]. " +
             "The volumes will be deleted when the instances terminate. " +
             "Only possible on EBS-backed AMIs. " +
             "EBS volumes are only attached if --ebs-vol-size > 0. " +
             "Only support up to 8 EBS volumes.")
    parser.add_option(
        "--placement-group", type="string", default=None,
        help="Which placement group to try and launch " +
             "instances into. Assumes placement group is already " +
             "created.")
    parser.add_option(
        "--swap", metavar="SWAP", type="int", default=1024,
        help="Swap space to set up per node, in MB (default: %default)")
    parser.add_option(
        "--spot-price", metavar="PRICE", type="float", default="0.05",
        help="If specified, launch slaves as spot instances with the given " +
             "maximum price (in dollars)")
    parser.add_option(
        "--master-spot-price", metavar="MASTER_PRICE", type="float", default="0.05",
        help="If specified, launch master as spot instance with the given " +
             "maximum price (in dollars)")
    parser.add_option(
        "--zeppelin-bucket", default="gezeppelin",
        help="the s3 bucket name to use for zeppelin notebooks")
    parser.add_option(
        "--pipeline-bucket", default="gedatalab",
        help="the s3 bucket name to use for pipeline binaries")
    parser.add_option(
        "--pipeline-version", default=DEFAULT_PIPELINE_VERSION,
        help="the version of pipeline to use")
    parser.add_option(
        "--ganglia", action="store_true", default=False,
        help="Setup Ganglia monitoring on cluster (default: %default). NOTE: " +
             "the Ganglia page will be publicly accessible")
    parser.add_option(
        "--no-ganglia", action="store_false", dest="ganglia",
        help="Disable Ganglia monitoring for the cluster")
    parser.add_option(
        "-u", "--user", default="root",
        help="The SSH user you want to connect as (default: %default)")
    parser.add_option(
        "--delete-groups", action="store_true", default=False,
        help="When destroying a cluster, delete the security groups that were created")
    parser.add_option(
        "--use-existing-master", action="store_true", default=False,
        help="Launch fresh slaves, but use an existing stopped master if possible")
    parser.add_option(
        "--worker-instances", type="int", default=1,
        help="Number of instances per worker: variable SPARK_WORKER_INSTANCES. Not used if YARN " +
             "is enabled (default: %default)")
    parser.add_option(
        "--master-opts", type="string", default="",
        help="Extra options to give to master through SPARK_MASTER_OPTS variable " +
             "(e.g -Dspark.worker.timeout=180)")
    parser.add_option(
        "--user-data", type="string", default="",
        help="Path to a user-data file (most AMIs interpret this as an initialization script)")
    parser.add_option(
        "--authorized-address", type="string", default="0.0.0.0/0",
        help="Address to authorize on created security groups (default: %default)")
    parser.add_option(
        "--additional-security-group", type="string", default="",
        help="Additional security group to place the machines in")
    parser.add_option(
        "--es-security-group", type="string", default="",
        help="Elasticsearch discovery security group, if unset local elasticsearch is not deployed")
    parser.add_option(
        "--additional-tags", type="string", default="",
        help="Additional tags to set on the machines; tags are comma-separated, while name and " +
             "value are colon separated; ex: \"Task:MySparkProject,Env:production\"")
    parser.add_option(
        "--copy-aws-credentials", action="store_true", default=True,
        help="Add AWS credentials to hadoop configuration to allow Spark to access S3")
    parser.add_option(
        "--subnet-id", default=None,
        help="VPC subnet to launch instances in")
    parser.add_option(
        "--vpc-id", default=None,
        help="VPC to launch instances in")
    parser.add_option(
        "--private-ips", action="store_true", default=False,
        help="Use private IPs for instances rather than public if VPC/subnet " +
             "requires that.")
    parser.add_option(
        "--instance-initiated-shutdown-behavior", default="stop",
        choices=["stop", "terminate"],
        help="Whether instances should terminate when shut down or just stop")
    parser.add_option(
        "--instance-profile-name", default="s3_full_access",
        help="IAM profile name to launch instances under")

    (opts, args) = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)
    (action, cluster_name) = args

    # Boto config check
    # http://boto.cloudhackers.com/en/latest/boto_config_tut.html
    home_dir = os.getenv('HOME')
    if home_dir is None or not os.path.isfile(home_dir + '/.boto'):
        if not os.path.isfile('/etc/boto.cfg'):
            # If there is no boto config, check aws credentials
            if not os.path.isfile(home_dir + '/.aws/credentials'):
                if os.getenv('AWS_ACCESS_KEY_ID') is None:
                    print("ERROR: The environment variable AWS_ACCESS_KEY_ID must be set",
                          file=stderr)
                    sys.exit(1)
                if os.getenv('AWS_SECRET_ACCESS_KEY') is None:
                    print("ERROR: The environment variable AWS_SECRET_ACCESS_KEY must be set",
                          file=stderr)
                    sys.exit(1)
    return (opts, action, cluster_name)


def get_or_make_group(conn, name, vpc_id):
    """Get the EC2 security group of the given name, creating it if it doesn't exist"""
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print("Creating security group " + name)
        return conn.create_security_group(name, "Spark EC2 group", vpc_id)


def get_validate_spark_version(version, repo):
    if "." in version:
        version = version.replace("v", "")
        if version not in VALID_SPARK_VERSIONS:
            print("Don't know about Spark version: {v}".format(v=version), file=stderr)
            sys.exit(1)
        return version
    else:
        github_commit_url = "{repo}/commit/{commit_hash}".format(repo=repo, commit_hash=version)
        request = Request(github_commit_url)
        request.get_method = lambda: 'HEAD'
        try:
            response = urlopen(request)
        except HTTPError as e:
            print("Couldn't validate Spark commit: {url}".format(url=github_commit_url),
                  file=stderr)
            print("Received HTTP response code of {code}.".format(code=e.code), file=stderr)
            sys.exit(1)
        return version


def get_tachyon_version(spark_version):
    """Find a Tachyon version matching the selected Spark version"""
    return SPARK_TACHYON_MAP.get(spark_version, "")


def get_zeppelin_version(spark_version):
    """Find a Zeppelin version matching the selected Spark version"""
    return SPARK_ZEPPELIN_MAP.get(spark_version, "")


def get_hadoop_version(spark_version):
    """Find a Zeppelin version matching the selected Spark version"""
    return SPARK_HADOOP_MAP.get(spark_version, "")


def get_scala_version(spark_version):
    """Find a Zeppelin version matching the selected Spark version"""
    return SPARK_SCALA_MAP.get(spark_version, "")


def get_spark_ami(opts):
    """Attempt to resolve an appropriate AMI given the architecture and region of the request."""
    if opts.instance_type in EC2_INSTANCE_TYPES:
        instance_type = EC2_INSTANCE_TYPES[opts.instance_type]
    else:
        instance_type = "pvm"
        print("Don't recognize %s, assuming type is pvm" % opts.instance_type, file=stderr)

    # URL prefix from which to fetch AMI information
    ami_prefix = "{r}/{b}/ami-list".format(
        r=opts.spark_ec2_git_repo.replace("https://github.com", "https://raw.github.com", 1),
        b=opts.spark_ec2_git_branch)

    ami_path = "%s/%s/%s" % (ami_prefix, opts.region, instance_type)
    reader = codecs.getreader("ascii")
    try:
        ami = reader(urlopen(ami_path)).read().strip()
    except:
        print("Could not resolve AMI at: " + ami_path, file=stderr)
        sys.exit(1)

    print("Spark AMI: " + ami)
    return ami


def launch_cluster(conn, opts, cluster_name):
    """ Launch a cluster of the given name, by setting up its security groups, and then starting new instances in them.
    Returns a tuple of EC2 reservation objects for the master and slaves
    Fails if there already instances running in the cluster's groups.
    """
    if opts.identity_file is None:
        print("ERROR: Must provide an identity file (-i) for ssh connections.", file=stderr)
        sys.exit(1)

    if opts.key_pair is None:
        print("ERROR: Must provide a key pair name (-k) to use on instances.", file=stderr)
        sys.exit(1)

    user_data_content = None
    if opts.user_data:
        with open(opts.user_data) as user_data_file:
            user_data_content = user_data_file.read()

    print("Setting up security groups...")
    master_group = get_or_make_group(conn, cluster_name + "-master", opts.vpc_id)
    slave_group = get_or_make_group(conn, cluster_name + "-slaves", opts.vpc_id)

    if master_group.rules == []:  # Group was just now created
        setup_acl(master_group, slave_group, opts.authorized_address, opts.vpc_id)

    if slave_group.rules == []:  # Group was just now created
        setup_acl(slave_group, master_group, opts.authorized_address, opts.vpc_id)

    # Check if instances are already running in our groups
    existing_masters, existing_slaves = get_existing_cluster(conn, opts, cluster_name, die_on_error=False)
    if (existing_slaves or existing_masters) and not opts.use_existing_master:
        print("ERROR: There are already instances running in group %s or %s and --use-existing-master not set" %
              (master_group.name, slave_group.name), file=stderr)
        sys.exit(1)

    if opts.use_existing_master and len(existing_masters) == 0:
        print("ERROR: No master exists for %s" % master_group.name, file=stderr)
        sys.exit(1)

    if opts.use_existing_master and len(existing_slaves) >= opts.slaves:
        print("ERROR: There are already enough slave instances (%d) running in group %s" %
              (len(existing_slaves), slave_group.name), file=stderr)
        sys.exit(1)

    opts.total_slaves = opts.slaves
    opts.slaves = opts.total_slaves - len(existing_slaves)

    # Figure out Spark AMI
    if opts.ami is None:
        opts.ami = get_spark_ami(opts)

    # we use group ids to work around https://github.com/boto/boto/issues/350
    additional_group_ids = []
    all_groups = conn.get_all_security_groups()
    if opts.additional_security_group:
        additional_group_ids += [sg.id for sg in all_groups if opts.additional_security_group in (sg.name, sg.id)]
    if opts.es_security_group:
        additional_group_ids += [sg.id for sg in all_groups if opts.es_security_group in (sg.name, sg.id)]
    print("Launching instances...")

    try:
        image = conn.get_all_images(image_ids=[opts.ami])[0]
    except:
        print("Could not find AMI " + opts.ami, file=stderr)
        sys.exit(1)

    block_map = build_block_device_map(
        ebs_vol_num=opts.ebs_vol_num,
        ebs_vol_size=opts.ebs_vol_size,
        ebs_vol_type=opts.ebs_vol_type,
        instance_type=opts.instance_type
    )

    # Launch slaves
    slave_nodes = existing_slaves
    if existing_slaves:
        print("Starting existing slaves if necessary...")
        for inst in existing_slaves:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()

    zones = get_zones(conn, opts)
    zone_partition = get_partition(opts.slaves, zones)
    if opts.spot_price is not None:
        print("Requesting %d slaves as spot instances with price $%.3f" % (opts.slaves, opts.spot_price))
    else:
        print("Requesting %d slaves as on demand instances" % opts.slaves)

    try:
        for zone in zone_partition:
            count = zone_partition[zone]
            print("Staring %d instances in %s" % (count, zone))
            if count > 0:
                if opts.spot_price is not None:
                    print ("Requesting spot instances...")
                    spot_instances = launch_spot_instances(
                        conn=conn,
                        price=opts.spot_price,
                        launch_group="launch-group-%s" % cluster_name,
                        placement=zone,
                        count=count,
                        security_group_ids=[slave_group.id] + additional_group_ids,
                        block_device_map=block_map,
                        instance_type=opts.instance_type,
                        image_id=opts.ami,
                        key_pair=opts.key_pair,
                        instance_profile_name=opts.instance_profile_name,
                        placement_group=opts.placement_group,
                        subnet_id=opts.subnet_id,
                        user_data=user_data_content
                    )
                    slave_nodes += spot_instances
                else:
                    slave_res = image.run(
                        key_name=opts.key_pair,
                        security_group_ids=[slave_group.id] + additional_group_ids,
                        instance_type=opts.instance_type,
                        placement=zone,
                        min_count=count,
                        max_count=count,
                        block_device_map=block_map,
                        subnet_id=opts.subnet_id,
                        placement_group=opts.placement_group,
                        user_data=user_data_content,
                        instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
                        instance_profile_name=opts.instance_profile_name)
                    slave_nodes += slave_res.instances
                print("Launched {s} slave{plural_s} in {z}".format(s=count,z=zone,plural_s=('' if count == 1 else 's')))
    except:
        # Log a warning if any of these requests actually launched instances:
        (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name, die_on_error=False)
        running = len(master_nodes) + len(slave_nodes)
        if running:
            print(("WARNING: %d instances are still running" % running), file=stderr)
        sys.exit(0)

    # Launch or resume masters
    master_nodes = []
    if existing_masters:
        print("Starting master...")
        for inst in existing_masters:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        master_nodes = existing_masters
    else:
        master_type = opts.master_instance_type
        if master_type == "":
            master_type = opts.instance_type
        if opts.zone == 'all':
            zone = random.choice(conn.get_all_zones()).name
        else:
            zone = opts.zone

        if opts.spot_price is not None:
            # Launch spot instance with the requested price
            print("Requesting master as spot instance with price $%.3f" % (opts.master_spot_price))
            master_nodes += launch_spot_instances(
                conn=conn,
                price=opts.master_spot_price,
                image_id=opts.ami,
                key_pair=opts.key_pair,
                launch_group="master-group-%s" % cluster_name,
                security_group_ids=[master_group.id] + additional_group_ids,
                instance_type=master_type,
                placement=zone,
                count=1,
                block_device_map=block_map,
                subnet_id=opts.subnet_id,
                placement_group=opts.placement_group,
                user_data=user_data_content,
                instance_profile_name=opts.instance_profile_name
            )
        else:
            master_res = image.run(
                key_name=opts.key_pair,
                security_group_ids=[master_group.id] + additional_group_ids,
                instance_type=master_type,
                placement=zone,
                min_count=1,
                max_count=1,
                block_device_map=block_map,
                subnet_id=opts.subnet_id,
                placement_group=opts.placement_group,
                user_data=user_data_content,
                instance_initiated_shutdown_behavior=opts.instance_initiated_shutdown_behavior,
                instance_profile_name=opts.instance_profile_name)

            master_nodes = master_res.instances
        print("Launched master in %s" % zone)

    # This wait time corresponds to SPARK-4983
    print("Waiting for AWS to propagate instance metadata...")
    time.sleep(15)

    # Give the instances descriptive names and set additional tags
    additional_tags = {}
    if opts.additional_tags.strip():
        additional_tags = dict(
            map(str.strip, tag.split(':', 1)) for tag in opts.additional_tags.split(',')
        )

    for node in master_nodes+slave_nodes:
        if node in master_nodes:
            role='master'
        else:
            role='slave'
        node.add_tags(
            dict(additional_tags, Name='{cn}-{role}-{iid}'.format(cn=cluster_name, iid=node.id, role=role))
        )

    # Return all the instances
    return (master_nodes, slave_nodes)


def build_block_device_map(ebs_vol_num,ebs_vol_size,ebs_vol_type,instance_type):
    """
    Create block device mapping so that we can add EBS volumes if asked to.
    The first drive is attached as /dev/sds, 2nd as /dev/sdt, ... /dev/sdz
    """
    block_map = BlockDeviceMapping()
    if ebs_vol_size > 0:
        for i in range(ebs_vol_num):
            device = EBSBlockDeviceType()
            device.size = ebs_vol_size
            device.volume_type = ebs_vol_type
            device.delete_on_termination = True
            block_map["/dev/sd" + chr(ord('s') + i)] = device

    # AWS ignores the AMI-specified block device mapping for M3 (see SPARK-3342).
    if instance_type.startswith('m3.'):
        for i in range(get_num_disks(instance_type)):
            dev = BlockDeviceType()
            dev.ephemeral_name = 'ephemeral%d' % i
            # The first ephemeral drive is /dev/sdb.
            name = '/dev/sd' + string.ascii_letters[i + 1]
            block_map[name] = dev

    return block_map


def launch_spot_instances(conn, launch_group, placement, price, instance_type, subnet_id, instance_profile_name,
                          placement_group, key_pair, image_id, security_group_ids, block_device_map,
                          user_data, count=1):
    print("Creating spot requests in %s for %d %s instances " % (placement, count, instance_type))
    my_req_ids=[]
    slave_reqs = conn.request_spot_instances(
        price=price,
        image_id=image_id,
        launch_group=launch_group,
        placement=placement,
        placement_group=placement_group,
        count=count,
        key_name=key_pair,
        security_group_ids=security_group_ids,
        instance_type=instance_type,
        block_device_map=block_device_map,
        subnet_id=subnet_id,
        user_data=user_data,
        instance_profile_name=instance_profile_name)
    my_req_ids += [req.id for req in slave_reqs]
    print("Waiting for spot instances to be granted...")
    try:
        while True:
            time.sleep(10)
            reqs = conn.get_all_spot_instance_requests()
            id_to_req = {}
            for r in reqs:
                id_to_req[r.id] = r
            active_instance_ids = []
            for i in my_req_ids:
                if i in id_to_req and id_to_req[i].state == "active":
                    active_instance_ids.append(id_to_req[i].instance_id)
            if len(active_instance_ids) == count:
                print("All %d instances granted" % count)
                reservations = conn.get_all_reservations(active_instance_ids)
                nodes = []
                for r in reservations:
                    nodes += r.instances
                break
            else:
                print("%d of %d instances granted, waiting longer" % (len(active_instance_ids), count))
    except:
        print("Canceling spot instance requests")
        conn.cancel_spot_instance_requests(my_req_ids)
        raise

    return nodes


def setup_acl(local_group, remote_group, authorized_address, vpc_id):
    if vpc_id is None:
        local_group.authorize(src_group=local_group)
        local_group.authorize(src_group=remote_group)
    else:
        local_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1, src_group=local_group)
        local_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535, src_group=local_group)
        local_group.authorize(ip_protocol='udp', from_port=0, to_port=65535, src_group=local_group)
        local_group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1, src_group=remote_group)
        local_group.authorize(ip_protocol='tcp', from_port=0, to_port=65535, src_group=remote_group)
        local_group.authorize(ip_protocol='udp', from_port=0, to_port=65535, src_group=remote_group)
    local_group.authorize('tcp', 22, 22, authorized_address)


def get_existing_cluster(conn, opts, cluster_name, die_on_error=True):
    """
    Get the EC2 instances in an existing cluster if available.
    Returns a tuple of lists of EC2 instance objects for the masters and slaves.
    """
    print("Searching for existing cluster {c} in region {r}...".format(
        c=cluster_name, r=opts.region))

    def get_instances(group_names):
        """
        Get all non-terminated instances that belong to any of the provided security groups.

        EC2 reservation filters and instance states are documented here:
            http://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html#options
        """
        reservations = conn.get_all_reservations(
            filters={"instance.group-name": group_names})
        instances = itertools.chain.from_iterable(r.instances for r in reservations)
        return [i for i in instances if i.state not in ["shutting-down", "terminated"]]

    master_instances = get_instances([cluster_name + "-master"])
    slave_instances = get_instances([cluster_name + "-slaves"])

    if any((master_instances, slave_instances)):
        print("Found {m} master{plural_m}, {s} slave{plural_s}.".format(
            m=len(master_instances),
            plural_m=('' if len(master_instances) == 1 else 's'),
            s=len(slave_instances),
            plural_s=('' if len(slave_instances) == 1 else 's')))

    if not master_instances and die_on_error:
        print("ERROR: Could not find a master for cluster {c} in region {r}.".format(
            c=cluster_name, r=opts.region), file=sys.stderr)
        sys.exit(1)

    return (master_instances, slave_instances)


# Deploy configuration files and run setup scripts on a newly launched
# or started EC2 cluster.
def setup_cluster(conn, master_nodes, slave_nodes, opts, deploy_ssh_key, cluster_name, all_slaves=None):

    master = get_dns_name(master_nodes[0], opts.private_ips)
    if deploy_ssh_key:
        print("Generating cluster's SSH key on master...")
        key_setup = """
          [ -f ~/.ssh/id_rsa ] ||
            (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa &&
             cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys)
        """
        ssh(master, opts, key_setup)
        dot_ssh_tar = ssh_read(master, opts, ['tar', 'c', '.ssh'])
        print("Transferring cluster's SSH key to slaves...")
        for slave in slave_nodes:
            slave_address = get_dns_name(slave, opts.private_ips)
            print(slave_address)
            ssh_write(slave_address, opts, ['tar', 'x'], dot_ssh_tar)

    modules = ['hdfs', 'spark', 'spark-standalone', 'mysql', 'zeppelin', 'pipeline']

    if opts.ganglia:
        modules.append('ganglia')

    if opts.es_security_group:
        modules.append('elasticsearch')

    if opts.tachyon:
        modules.append('tachyon')

    if opts.yarn:
        modules.append('yarn')

    # Clear SPARK_WORKER_INSTANCES if running on YARN
    if opts.yarn:
        opts.worker_instances = ""

    # NOTE: We should clone the repository before running deploy_files to
    # prevent ec2-variables.sh from being overwritten
    print("Cloning spark-ec2 scripts from {r}/tree/{b} on master...".format(
        r=opts.spark_ec2_git_repo, b=opts.spark_ec2_git_branch))
    ssh(
        host=master,
        opts=opts,
        command="rm -rf spark-ec2"
                + " && "
                + "git clone {r} -b {b} spark-ec2".format(r=opts.spark_ec2_git_repo,
                                                          b=opts.spark_ec2_git_branch)
    )

    deploy_root = SPARK_EC2_DIR + "/" + "deploy."+opts.deploy_env
    print("Deploying root env {s} to master...".format(s=deploy_root))
    deploy_files(
        conn=conn,
        root_dir=deploy_root,
        opts=opts,
        master_nodes=master_nodes,
        slave_nodes=slave_nodes,
        modules=modules,
        cluster_name=cluster_name
    )

    if opts.deploy_root_dir is not None:
        print("Deploying {s} to master...".format(s=opts.deploy_root_dir))
        deploy_user_files(
            root_dir=opts.deploy_root_dir,
            opts=opts,
            master_nodes=master_nodes
        )

    print("Running setup on master...")
    setup_spark_cluster(master, opts)
    print("Done!")


def setup_spark_cluster(master, opts):
    ssh(master, opts, "chmod u+x spark-ec2/setup.sh")
    ssh(master, opts, "spark-ec2/setup.sh")
    print("Spark standalone cluster started at http://%s:4040" % master)

    if opts.ganglia:
        print("Ganglia started at http://%s:5080/ganglia" % master)


def is_ssh_available(host, opts, print_ssh_output=True):
    """
    Check if SSH is available on a host.
    """
    s = subprocess.Popen(
        ssh_command(opts) + ['-t', '-t', '-o', 'ConnectTimeout=3',
                             '%s@%s' % (opts.user, host), stringify_command('true')],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT  # we pipe stderr through stdout to preserve output order
    )
    cmd_output = s.communicate()[0]  # [1] is stderr, which we redirected to stdout

    if s.returncode != 0 and print_ssh_output:
        # extra leading newline is for spacing in wait_for_cluster_state()
        print(textwrap.dedent("""\n
            Warning: SSH connection error. (This could be temporary.)
            Host: {h}
            SSH return code: {r}
            SSH output: {o}
        """).format(
            h=host,
            r=s.returncode,
            o=cmd_output.strip()
        ))

    return s.returncode == 0


def is_cluster_ssh_available(cluster_instances, opts):
    """
    Check if SSH is available on all the instances in a cluster.
    """
    for i in cluster_instances:
        dns_name = get_dns_name(i, opts.private_ips)
        if not is_ssh_available(host=dns_name, opts=opts):
            return False
    else:
        return True


def wait_for_cluster_state(conn, opts, cluster_instances, cluster_state):
    """
    Wait for all the instances in the cluster to reach a designated state.

    cluster_instances: a list of boto.ec2.instance.Instance
    cluster_state: a string representing the desired state of all the instances in the cluster
           value can be 'ssh-ready' or a valid value from boto.ec2.instance.InstanceState such as
           'running', 'terminated', etc.
           (would be nice to replace this with a proper enum: http://stackoverflow.com/a/1695250)
    """
    sys.stdout.write(
        "Waiting for cluster to enter '{s}' state.".format(s=cluster_state)
    )
    sys.stdout.flush()

    start_time = datetime.now()
    num_attempts = 0

    while True:
        time.sleep(10 + (3 * num_attempts))  # seconds

        for i in cluster_instances:
            i.update()

        max_batch = 100
        statuses = []
        for j in xrange(0, len(cluster_instances), max_batch):
            batch = [i.id for i in cluster_instances[j:j + max_batch]]
            statuses.extend(conn.get_all_instance_status(instance_ids=batch))

        if cluster_state == 'ssh-ready':
            if all(i.state == 'running' for i in cluster_instances) and \
                    all(s.system_status.status == 'ok' for s in statuses) and \
                    all(s.instance_status.status == 'ok' for s in statuses) and \
                    is_cluster_ssh_available(cluster_instances, opts):
                break
        else:
            if all(i.state == cluster_state for i in cluster_instances):
                break

        num_attempts += 1

        sys.stdout.write(".")
        sys.stdout.flush()

    sys.stdout.write("\n")

    end_time = datetime.now()
    print("Cluster is now in '{s}' state. Waited {t} seconds.".format(
        s=cluster_state,
        t=(end_time - start_time).seconds
    ))


# Get number of local disks available for a given EC2 instance type.
def get_num_disks(instance_type):
    # Source: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html
    # Last Updated: 2015-06-19
    # For easy maintainability, please keep this manually-inputted dictionary sorted by key.
    disks_by_instance = {
        "c1.medium":   1,
        "c1.xlarge":   4,
        "c3.large":    2,
        "c3.xlarge":   2,
        "c3.2xlarge":  2,
        "c3.4xlarge":  2,
        "c3.8xlarge":  2,
        "c4.large":    0,
        "c4.xlarge":   0,
        "c4.2xlarge":  0,
        "c4.4xlarge":  0,
        "c4.8xlarge":  0,
        "cc1.4xlarge": 2,
        "cc2.8xlarge": 4,
        "cg1.4xlarge": 2,
        "cr1.8xlarge": 2,
        "d2.xlarge":   3,
        "d2.2xlarge":  6,
        "d2.4xlarge":  12,
        "d2.8xlarge":  24,
        "g2.2xlarge":  1,
        "g2.8xlarge":  2,
        "hi1.4xlarge": 2,
        "hs1.8xlarge": 24,
        "i2.xlarge":   1,
        "i2.2xlarge":  2,
        "i2.4xlarge":  4,
        "i2.8xlarge":  8,
        "m1.small":    1,
        "m1.medium":   1,
        "m1.large":    2,
        "m1.xlarge":   4,
        "m2.xlarge":   1,
        "m2.2xlarge":  1,
        "m2.4xlarge":  2,
        "m3.medium":   1,
        "m3.large":    1,
        "m3.xlarge":   2,
        "m3.2xlarge":  2,
        "m4.large":    0,
        "m4.xlarge":   0,
        "m4.2xlarge":  0,
        "m4.4xlarge":  0,
        "m4.10xlarge": 0,
        "r3.large":    1,
        "r3.xlarge":   1,
        "r3.2xlarge":  1,
        "r3.4xlarge":  1,
        "r3.8xlarge":  2,
        "t1.micro":    0,
        "t2.micro":    0,
        "t2.small":    0,
        "t2.medium":   0,
        "t2.large":    0,
    }
    if instance_type in disks_by_instance:
        return disks_by_instance[instance_type]
    else:
        print("WARNING: Don't know number of disks on instance type %s; assuming 1"
              % instance_type, file=stderr)
        return 1


# Deploy the configuration file templates in a given local directory to
# a cluster, filling in any template parameters with information about the
# cluster (e.g. lists of masters and slaves). Files are only deployed to
# the first master instance in the cluster, and we expect the setup
# script to be run on that instance to copy them to other nodes.
#
# root_dir should be an absolute path to the directory with the files we want to deploy.
def deploy_files(conn, root_dir, opts, master_nodes, slave_nodes, modules, cluster_name):
    active_master = get_dns_name(master_nodes[0], opts.private_ips)

    num_disks = get_num_disks(opts.instance_type)
    hdfs_data_dirs = "/mnt/hdfs/data"
    mapred_local_dirs = "/mnt/hadoop/mrlocal"
    spark_local_dirs = "/mnt/spark"
    if num_disks > 1:
        for i in range(2, num_disks + 1):
            hdfs_data_dirs += ",/mnt%d/hdfs/data" % i
            mapred_local_dirs += ",/mnt%d/hadoop/mrlocal" % i
            spark_local_dirs += ",/mnt%d/spark" % i

    cluster_url = "%s:7077" % active_master
    if "." in opts.spark_version:
        # Pre-built Spark deploy
        spark_v = get_validate_spark_version(opts.spark_version, opts.spark_git_repo)
        tachyon_v = get_tachyon_version(spark_v)
        zeppelin_v = get_zeppelin_version(spark_v)
        hadoop_v = get_hadoop_version(spark_v)
    else:
        # Spark-only custom deploy
        spark_v = "%s|%s" % (opts.spark_git_repo, opts.spark_version)
        tachyon_v = ""
        hadoop_v = "2.4"
        print("Deploying Spark via git hash; Tachyon won't be set up")
        modules = filter(lambda x: x != "tachyon", modules)

    if opts.scala_version:
        scala_v = opts.scala_version
    else:
        scala_v = get_scala_version(spark_v)

    master_addresses = [get_dns_name(i, opts.private_ips) for i in master_nodes]
    slave_addresses = [get_dns_name(i, opts.private_ips) for i in slave_nodes]
    worker_instances_str = "%d" % opts.worker_instances if opts.worker_instances else ""
    template_vars = {
        "cluster_name": cluster_name,
        "region": opts.region,
        "master_list": '\n'.join(master_addresses),
        "active_master": active_master,
        "slave_list": '\n'.join(slave_addresses),
        "cluster_url": cluster_url,
        "hdfs_data_dirs": hdfs_data_dirs,
        "mapred_local_dirs": mapred_local_dirs,
        "spark_local_dirs": spark_local_dirs,
        "swap": str(opts.swap),
        "modules": '\n'.join(modules),
        "spark_version": spark_v,
        "tachyon_version": tachyon_v,
        "scala_version": scala_v,
        "hadoop_major_version": hadoop_v,
        "zeppelin_version": zeppelin_v,
        "zeppelin_bucket": opts.zeppelin_bucket,
        "pipeline_version": opts.pipeline_version,
        "pipeline_bucket": opts.pipeline_bucket,
        "spark_worker_instances": worker_instances_str,
        "spark_master_opts": opts.master_opts,
    }

    if opts.copy_aws_credentials:
        template_vars["aws_access_key_id"] = opts.aws_access_key_id
        template_vars["aws_secret_access_key"] = opts.aws_secret_access_key
    else:
        template_vars["aws_access_key_id"] = ""
        template_vars["aws_secret_access_key"] = ""

    if opts.es_security_group:
        template_vars["es_discovery"]=opts.es_security_group
        template_vars["es_clustername"]=opts.es_security_group.split('-')[0]

    # Create a temp directory in which we will place all the files to be
    # deployed after we substitue template parameters in them
    tmp_dir = tempfile.mkdtemp()
    for path, dirs, files in os.walk(root_dir):
        if path.find(".svn") == -1:
            dest_dir = os.path.join('/', path[len(root_dir):])
            local_dir = tmp_dir + dest_dir
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            for filename in files:
                if filename[0] not in '#.~' and filename[-1] != '~':
                    dest_file = os.path.join(dest_dir, filename)
                    local_file = tmp_dir + dest_file
                    with open(os.path.join(path, filename)) as src:
                        with open(local_file, "w") as dest:
                            text = src.read()
                            for key in template_vars:
                                text = text.replace("{{" + key + "}}", template_vars[key])
                            dest.write(text)
                            dest.close()
    # rsync the whole directory over to the master machine
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s/" % tmp_dir,
        "%s@%s:/" % (opts.user, active_master)
    ]
    subprocess.check_call(command)
    # Remove the temp directory we created above
    shutil.rmtree(tmp_dir)


# Deploy a given local directory to a cluster, WITHOUT parameter substitution.
# Note that unlike deploy_files, this works for binary files.
# Also, it is up to the user to add (or not) the trailing slash in root_dir.
# Files are only deployed to the first master instance in the cluster.
#
# root_dir should be an absolute path.
def deploy_user_files(root_dir, opts, master_nodes):
    active_master = get_dns_name(master_nodes[0], opts.private_ips)
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s" % root_dir,
        "%s@%s:/" % (opts.user, active_master)
    ]
    subprocess.check_call(command)


def stringify_command(parts):
    if isinstance(parts, str):
        return parts
    else:
        return ' '.join(map(pipes.quote, parts))


def ssh_args(opts):
    parts = ['-o', 'StrictHostKeyChecking=no']
    parts += ['-o', 'UserKnownHostsFile=/dev/null']
    if opts.identity_file is not None:
        parts += ['-i', opts.identity_file]
    return parts


def ssh_command(opts):
    return ['ssh'] + ssh_args(opts)


# Run a command on a host through ssh, retrying up to five times
# and then throwing an exception if ssh continues to fail.
def ssh(host, opts, command):
    tries = 0
    while True:
        try:
            return subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '%s@%s' % (opts.user, host),
                                     stringify_command(command)])
        except subprocess.CalledProcessError as e:
            if tries > 5:
                # If this was an ssh failure, provide the user with hints.
                if e.returncode == 255:
                    raise UsageError(
                        "Failed to SSH to remote host {0}.\n"
                        "Please check that you have provided the correct --identity-file and "
                        "--key-pair parameters and try again.".format(host))
                else:
                    raise e
            print("Error executing remote command, retrying after 30 seconds: {0}".format(e),
                  file=stderr)
            time.sleep(30)
            tries = tries + 1


# Backported from Python 2.7 for compatiblity with 2.6 (See SPARK-1990)
def _check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise subprocess.CalledProcessError(retcode, cmd, output=output)
    return output


def ssh_read(host, opts, command):
    return _check_output(
        ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)])


def ssh_write(host, opts, command, arguments):
    tries = 0
    while True:
        proc = subprocess.Popen(
            ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)],
            stdin=subprocess.PIPE)
        proc.stdin.write(arguments)
        proc.stdin.close()
        status = proc.wait()
        if status == 0:
            break
        elif tries > 5:
            raise RuntimeError("ssh_write failed with error %s" % proc.returncode)
        else:
            print("Error {0} while executing remote command, retrying after 30 seconds".
                  format(status), file=stderr)
            time.sleep(30)
            tries = tries + 1


def get_zones(conn, opts):
    """Gets a list of zones to launch instances in"""
    if opts.zone == 'all':
        zones = [z.name for z in conn.get_all_zones()]
    else:
        zones = [opts.zone]
    return zones


def get_partition(total, partitions):
    """Returns a partition of the total requested split between the partitions list elements """
    num_partitions = len(partitions)
    partition_map = dict()
    for idx, partition_id in enumerate(partitions):
        num_elems_in_partition = total // num_partitions
        if (total % num_partitions) - idx > 0:
            num_elems_in_partition += 1
        partition_map[partition_id] = num_elems_in_partition

    return partition_map


def get_ip_address(instance, private_ips=False):
    """Gets the IP address, taking into account the --private-ips flag"""
    ip = instance.ip_address if not private_ips else \
        instance.private_ip_address
    return ip


def get_dns_name(instance, private_ips=False):
    """Gets the DNS name, taking into account the --private-ips flag"""
    dns = instance.public_dns_name if not private_ips else instance.private_ip_address
    return dns


def real_main():
    (opts, action, cluster_name) = parse_args()

    # Input parameter validation
    get_validate_spark_version(opts.spark_version, opts.spark_git_repo)

    if opts.identity_file is not None:
        if not os.path.exists(opts.identity_file):
            print("ERROR: The identity file '{f}' doesn't exist.".format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

        file_mode = os.stat(opts.identity_file).st_mode
        if not (file_mode & S_IRUSR) or not oct(file_mode)[-2:] == '00':
            print("ERROR: The identity file must be accessible only by you.", file=stderr)
            print('You can fix this with: chmod 400 "{f}"'.format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

    if opts.instance_type not in EC2_INSTANCE_TYPES:
        print("Warning: Unrecognized EC2 instance type for instance-type: {t}".format(
            t=opts.instance_type), file=stderr)

    if opts.master_instance_type != "":
        if opts.master_instance_type not in EC2_INSTANCE_TYPES:
            print("Warning: Unrecognized EC2 instance type for master-instance-type: {t}".format(
                t=opts.master_instance_type), file=stderr)
        # Since we try instance types even if we can't resolve them, we check if they resolve first
        # and, if they do, see if they resolve to the same virtualization type.
        if opts.instance_type in EC2_INSTANCE_TYPES and \
                        opts.master_instance_type in EC2_INSTANCE_TYPES:
            if EC2_INSTANCE_TYPES[opts.instance_type] != \
                    EC2_INSTANCE_TYPES[opts.master_instance_type]:
                print("Error: spark-ec2 currently does not support having a master and slaves "
                      "with different AMI virtualization types.", file=stderr)
                print("master instance virtualization type: {t}".format(
                    t=EC2_INSTANCE_TYPES[opts.master_instance_type]), file=stderr)
                print("slave instance virtualization type: {t}".format(
                    t=EC2_INSTANCE_TYPES[opts.instance_type]), file=stderr)
                sys.exit(1)

    if opts.ebs_vol_num > 8:
        print("ebs-vol-num cannot be greater than 8", file=stderr)
        sys.exit(1)

    # Prevent breaking ami_prefix (/, .git and startswith checks)
    # Prevent forks with non spark-ec2 names for now.
    if opts.spark_ec2_git_repo.endswith("/") or \
            opts.spark_ec2_git_repo.endswith(".git") or \
            not opts.spark_ec2_git_repo.startswith("https://github.com") or \
            not opts.spark_ec2_git_repo.endswith("spark-ec2"):
        print("spark-ec2-git-repo must be a github repo and it must not have a trailing / or .git. "
              "Furthermore, we currently only support forks named spark-ec2.", file=stderr)
        sys.exit(1)

    if not (opts.deploy_root_dir is None or
                (os.path.isabs(opts.deploy_root_dir) and
                     os.path.isdir(opts.deploy_root_dir) and
                     os.path.exists(opts.deploy_root_dir))):
        print("--deploy-root-dir must be an absolute path to a directory that exists "
              "on the local file system", file=stderr)
        sys.exit(1)

    try:
        if opts.profile is None:
            conn = ec2.connect_to_region(opts.region)
        else:
            conn = ec2.connect_to_region(opts.region, profile_name=opts.profile)
    except Exception as e:
        print((e), file=stderr)
        sys.exit(1)

    if opts.deploy_aws_key_id is None:
        opts.aws_access_key_id = conn.aws_access_key_id
    else:
        opts.aws_access_key_id = opts.deploy_aws_key_id

    if opts.deploy_aws_key_secret is None:
        opts.aws_secret_access_key = conn.aws_secret_access_key
    else:
        opts.aws_secret_access_key = opts.deploy_aws_key_secret

    try:
        if opts.deploy_profile is not None:
            deployconn = ec2.connect_to_region(opts.region, profile_name=opts.deploy_profile)
            opts.aws_access_key_id = deployconn.aws_access_key_id
            opts.aws_secret_access_key = deployconn.aws_secret_access_key
    except Exception as e:
        print((e), file=stderr)

    # Select an AZ at random if it was not specified.
    if opts.zone == "":
        opts.zone = random.choice(conn.get_all_zones()).name

    if action == "launch":
        if opts.slaves <= 0:
            print("ERROR: You have to start at least 1 slave", file=sys.stderr)
            sys.exit(1)
        if opts.resume:
            (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
        else:
            (master_nodes, slave_nodes) = launch_cluster(conn, opts, cluster_name)
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=(master_nodes + slave_nodes),
            cluster_state='ssh-ready'
        )
        setup_cluster(conn, master_nodes, slave_nodes, opts, True, cluster_name)

    elif action == "destroy":
        (master_nodes, slave_nodes) = get_existing_cluster(
            conn, opts, cluster_name, die_on_error=False)

        if any(master_nodes + slave_nodes):
            print("The following instances will be terminated:")
            for inst in master_nodes + slave_nodes:
                print("> %s" % get_dns_name(inst, opts.private_ips))
            print("ALL DATA ON ALL NODES WILL BE LOST!!")

        msg = "Are you sure you want to destroy the cluster {c}? (y/N) ".format(c=cluster_name)
        response = raw_input(msg)
        if response == "y":
            print("Terminating master...")
            for inst in master_nodes:
                inst.terminate()
            print("Terminating slaves...")
            for inst in slave_nodes:
                inst.terminate()

            # Delete security groups as well
            if opts.delete_groups:
                group_names = [cluster_name + "-master", cluster_name + "-slaves"]
                wait_for_cluster_state(
                    conn=conn,
                    opts=opts,
                    cluster_instances=(master_nodes + slave_nodes),
                    cluster_state='terminated'
                )
                print("Deleting security groups (this will take some time)...")
                attempt = 1
                while attempt <= 3:
                    print("Attempt %d" % attempt)
                    groups = [g for g in conn.get_all_security_groups() if g.name in group_names]
                    success = True
                    # Delete individual rules in all groups before deleting groups to
                    # remove dependencies between them
                    for group in groups:
                        print("Deleting rules in security group " + group.name)
                        for rule in group.rules:
                            for grant in rule.grants:
                                success &= group.revoke(ip_protocol=rule.ip_protocol,
                                                        from_port=rule.from_port,
                                                        to_port=rule.to_port,
                                                        src_group=grant)

                    # Sleep for AWS eventual-consistency to catch up, and for instances
                    # to terminate
                    time.sleep(30)  # Yes, it does have to be this long :-(
                    for group in groups:
                        try:
                            # It is needed to use group_id to make it work with VPC
                            conn.delete_security_group(group_id=group.id)
                            print("Deleted security group %s" % group.name)
                        except boto.exception.EC2ResponseError:
                            success = False
                            print("Failed to delete security group %s" % group.name)

                    # Unfortunately, group.revoke() returns True even if a rule was not
                    # deleted, so this needs to be rerun if something fails
                    if success:
                        break

                    attempt += 1

                if not success:
                    print("Failed to delete all security groups after 3 tries.")
                    print("Try re-running in a few minutes.")

    elif action == "login":
        (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
        if not master_nodes[0].public_dns_name and not opts.private_ips:
            print("Master has no public DNS name.  Maybe you meant to specify --private-ips?")
        else:
            master = get_dns_name(master_nodes[0], opts.private_ips)
            print("Logging into master " + master + "...")
            proxy_opt = []
            if opts.proxy_port is not None:
                proxy_opt = ['-D', opts.proxy_port]
            subprocess.check_call(
                ssh_command(opts) + proxy_opt + ['-t', '-t', "%s@%s" % (opts.user, master)])

    elif action == "reboot-slaves":
        response = raw_input(
            "Are you sure you want to reboot the cluster " +
            cluster_name + " slaves?\n" +
            "Reboot cluster slaves " + cluster_name + " (y/N): ")
        if response == "y":
            (master_nodes, slave_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            print("Rebooting slaves...")
            for inst in slave_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    print("Rebooting " + inst.id)
                    inst.reboot()

    elif action == "get-master":
        (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
        if not master_nodes[0].public_dns_name and not opts.private_ips:
            print("Master has no public DNS name.  Maybe you meant to specify --private-ips?")
        else:
            print(get_dns_name(master_nodes[0], opts.private_ips))

    elif action == "stop":
        response = raw_input(
            "Are you sure you want to stop the cluster " +
            cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
            "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" +
            "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
            "All data on spot-instance slaves will be lost.\n" +
            "Stop cluster " + cluster_name + " (y/N): ")
        if response == "y":
            (master_nodes, slave_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            print("Stopping master...")
            for inst in master_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    inst.stop()
            print("Stopping slaves...")
            for inst in slave_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    if inst.spot_instance_request_id:
                        inst.terminate()
                    else:
                        inst.stop()

    elif action == "start":
        (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
        print("Starting slaves...")
        for inst in slave_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        print("Starting master...")
        for inst in master_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=(master_nodes + slave_nodes),
            cluster_state='ssh-ready'
        )

        # Determine types of running instances
        existing_master_type = master_nodes[0].instance_type
        existing_slave_type = slave_nodes[0].instance_type
        # Setting opts.master_instance_type to the empty string indicates we
        # have the same instance type for the master and the slaves
        if existing_master_type == existing_slave_type:
            existing_master_type = ""
        opts.master_instance_type = existing_master_type
        opts.instance_type = existing_slave_type

        setup_cluster(conn, master_nodes, slave_nodes, opts, False, cluster_name)

    else:
        print("Invalid action: %s" % action, file=stderr)
        sys.exit(1)


def main():
    try:
        real_main()
    except UsageError as e:
        print("\nError:\n", e, file=stderr)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig()
    main()
