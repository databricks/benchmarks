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

from contextlib import contextmanager
import logging
import pyarrow.parquet as pq
import time
import argparse
from glob import glob

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
parser = argparse.ArgumentParser(description='Pandas Query Benchmark')
parser.add_argument('path', metavar='path',
                    help='Path of TPC-DS Parquet file, e.g. /data/tpcds250/store_sales')
args = parser.parse_args()
files = glob(args.path + '/*/*.parquet')
with time_usage("read parquet file"):
    df = pq.ParquetDataset(files).read(nthreads=32).to_pandas()

with time_usage("Q1: sum of single column"):
    df[['ss_customer_sk']].sum()
with time_usage("Q2: distinct count"):
    df['ss_customer_sk'].nunique()
with time_usage("Q3: sum of single column with group by"):
    df[['ss_store_sk', 'ss_net_profit']].groupby('ss_store_sk').sum()
