# Copyright 2018 Databricks, Inc.
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
from pyspark.sql import SparkSession
from contextlib import contextmanager
import time
import logging
import argparse

@contextmanager
def time_usage(name=""):
    """log the time usage in a code block
    prefix: the prefix text to show
    """
    start = time.time()
    yield
    end = time.time()
    elapsed_seconds = float("%.4f" % (end - start))
    logging.info('%s: elapsed seconds: %s', name, elapsed_seconds)


logging.getLogger().setLevel(logging.INFO)

spark = SparkSession.builder.appName("pyspark_benchmark").getOrCreate()
parser = argparse.ArgumentParser(description='PySpark Query Benchmark')
parser.add_argument('path', metavar='path',
                    help='Path of TPC-DS Parquet file, e.g. /data/tpcds250/store_sales')
args = parser.parse_args()
with time_usage("read file"):
    df = spark.read.parquet(args.path)

df.createOrReplaceTempView("t")

# Loop to warm up JVM
for i in range(1, 10):
    with time_usage("sum of single column"):
        spark.sql("select sum(ss_customer_sk) from t").collect()
    with time_usage("aggregate on single column"):
        spark.sql("select count(distinct ss_customer_sk) from t").collect()
    with time_usage("aggregate on single column"):
        spark.sql("select sum(ss_net_profit) from t group by ss_store_sk").collect()
