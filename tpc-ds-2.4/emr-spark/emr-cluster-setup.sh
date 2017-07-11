#!/bin/bash

# Copyright 2017 Databricks, Inc.
#
# This work (the "Licensed Material") is licensed under the Creative Commons
# Attribution-NonCommercial-NoDerivatives 4.0 International License. You may
# not use this file except in compliance with the License.
#
# To view a copy of this license, visit https://creativecommons.org/licenses/by-nc-nd/4.0/
#
# Unless required by applicable law or agreed to in writing, the Licensed Material is offered
# on an "AS-IS" and "AS-AVAILABLE" BASIS, WITHOUT REPRESENTATIONS OR WARRANTIES OF ANY KIND,
# whether express, implied, statutory, or other. This includes, without limitation, warranties
# of title, merchantability, fitness for a particular purpose, non-infringement, absence of
# latent or other defects, accuracy, or the presence or absence of errors, whether or not known
# or discoverable. To the extent legally permissible, in no event will the Licensor be liable
# to You on any legal theory (including, without limitation, negligence) or otherwise for
# any direct, special, indirect, incidental, consequential, punitive, exemplary, or other
# losses, costs, expenses, or damages arising out of this License or use of the Licensed
# Material, even if the Licensor has been advised of the possibility of such losses, costs,
# expenses, or damages.

for req in aws jq; do
  if [ -z $(which $req) ]; then
    echo "Missing required tool: $req"
    exit 1
  fi
done

if [ $# -ne 7 ]; then
  echo "Usage:   $0 <cluster-name> <num_workers> <instance_type> <bid_price> <keypair_name> <keypair_path> <spark-sql-perf_assembly>"
  echo "Example: $0 adrian-emr-test 10 r3.xlarge 0.333 adrian-emr-keypair ~/.ssh/.adrian-emr-keypair.pem /tmp/spark-sql-perf-assembly-0.4.12-SNAPSHOT.jar"
  exit 2
fi

ROOT_DIR=$(dirname $0)

CLUSTER_NAME=$1; shift
NUM_WORKERS=$1; shift
INSTANCE_TYPE=$1; shift
BID_PRICE=$1; shift
KEYPAIR_NAME=$1; shift
KEYPAIR_PATH=$1; shift
SPARK_SQL_PERF_ASSEMBLY=$1; shift

SPARK_DEFAULTS_PATH=$ROOT_DIR/spark-defaults.json
if [ ! -f $SPARK_DEFAULTS_PATH ]; then
  echo "Missing $SPARK_DEFAULTS_PATH file."
  exit 3
fi
SPARK_DEFAULTS=$(cat $SPARK_DEFAULTS_PATH)

TPCDS_SCRIPT_PATH=$ROOT_DIR/tpcds-wrappers.scala
if [ ! -f $TPCDS_SCRIPT_PATH ]; then
  echo "Missing $TPCDS_SCRIPT_PATH file."
  exit 3
fi

set -e

CLUSTER_ID=$(aws emr create-cluster \
  --name "${CLUSTER_NAME}" \
  --applications Name=Spark Name=Hive Name=Presto \
  --service-role EMR_DefaultRole \
  --release-label emr-5.6.0 \
  --region us-west-2 \
  --ec2-attributes '{
      "KeyName": "'${KEYPAIR_NAME}'",
      "InstanceProfile": "EMR_EC2_DefaultRole"
    }' \
  --instance-groups '[
      {
        "Name": "Master",
        "InstanceGroupType": "MASTER",
        "InstanceCount": 1,
        "InstanceType": "'${INSTANCE_TYPE}'"
      },
      {
        "Name": "Workers",
        "InstanceGroupType": "CORE",
        "InstanceCount": '${NUM_WORKERS}',
        "InstanceType": "'${INSTANCE_TYPE}'",
        "BidPrice": "'${BID_PRICE}'"
      }
    ]' \
  --configurations='[
    {
      "Classification": "yarn-site",
      "Properties": {
        "yarn.nodemanager.aux-services": "mapreduce_shuffle,spark_shuffle",
        "yarn.nodemanager.aux-services.mapreduce_shuffle.class": "org.apache.hadoop.mapred.ShuffleHandler",
        "yarn.nodemanager.aux-services.spark_shuffle.class": "org.apache.spark.network.yarn.YarnShuffleService"
      }
    },
    {
      "Classification": "spark",
      "Properties": {
        "maximizeResourceAllocation": "true"
      }
    },
    {
      "Classification": "spark-defaults",
      "Properties": '"${SPARK_DEFAULTS}"'
    }
  ]')

CLUSTER_ID=$(echo ${CLUSTER_ID} | jq -r '.ClusterId')
echo "Setting up cluster ${CLUSTER_ID} ..."

aws emr wait cluster-running --cluster-id ${CLUSTER_ID} && \
  notify-send "Your EMR Spark cluster is ready!"

CLUSTER_DESC=$(aws emr describe-cluster --cluster-id "${CLUSTER_ID}")
MASTER=$(echo ${CLUSTER_DESC} | jq -r  '.Cluster.MasterPublicDnsName')

CLUSTER_DESC_FILE=$(mktemp)
echo "$CLUSTER_DESC" | tr '\n' ' ' > $CLUSTER_DESC_FILE

SSH_OPTS="-i $KEYPAIR_PATH -oStrictHostKeyChecking=no"

scp ${SSH_OPTS} \
  $TPCDS_SCRIPT_PATH $SPARK_SQL_PERF_ASSEMBLY \
  hadoop@${MASTER}:

scp ${SSH_OPTS} \
  $CLUSTER_DESC_FILE \
  hadoop@${MASTER}:cluster-desc.json

ssh ${SSH_OPTS} hadoop@${MASTER} \
  hdfs dfs -put cluster-desc.json .

MASTER_IP=$(ssh ${SSH_OPTS} hadoop@${MASTER} hostname --ip-address)


echo
echo "Done setting up cluster. You can now ssh into the master node using the following command:"
echo
echo "   ssh -L 8042:$MASTER_IP:8042 -L 8088:$MASTER_IP:8088 -L 20888:$MASTER_IP:20888 -i $KEYPAIR_PATH hadoop@${MASTER}"
echo
echo "Once you're there, you can issue the following command to launch a spark-shell:"
echo
echo "   MASTER=yarn-client spark-shell --jars ~/$(basename $SPARK_SQL_PERF_ASSEMBLY) -i ~/$(basename $TPCDS_SCRIPT_PATH)"
echo
