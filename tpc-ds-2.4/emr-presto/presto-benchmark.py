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

import os
import subprocess
import sys
import time
from sets import Set

# DATABASE SCHEMA HAS TO BE CREATED IN HIVE FIRST.

# ------------------------------ Parameters
# Scale factor
scaleFactor = 1
# Hive catalog schema to use (create the database using Spark first!)
databaseName = "tpcds_sf%d" % (scaleFactor)
# Timeout of a query
timeout = '15m'
# Number of runs.
num_runs = 1
# Location of the folder that contains files with queries we will run
query_dir = "tpcds_2_4_presto"
# Directory to save results.
results_dir = "results/timestamp=%d" % (int(time.time()))
# The following are just for convert_raw_results CSV output:
vendor = "EMR"
system = "Presto 0.170"
cluster = "10x r3.xlarge (+r3.xlarge driver)"
configuration = "std"
date = time.strftime("%Y-%m-%d")
# ------------------------------


# Runs a command on a remote machine and checks the return code if needed
def run_command(cmd, verify_success=False):
    rv = subprocess.call(cmd, shell=True)
    if verify_success:
        assert rv == 0, "Command '%s' failed!" % cmd


def run_presto_benchmark(query_dir, results_dir, num_runs):
    run_command("rm -f __tmp*", True)
    run_command("mkdir -p %s/runs" % (results_dir), True)
    run_command("mkdir -p %s/plans" % (results_dir), True)

    # Create helper files.
    run_command("""echo \"""" +
                """select split_part(query,'---',2), """
                """date_diff('millisecond',started,\\"end\\"), state """ +
                """from system.runtime.queries order by \\"end\\" desc limit 1;" """ +
                """ > __tmp_get_runtime_from_presto.sql""")
    run_command("""echo "set session query_max_run_time = '%s';" > __tmp_presto_configs.sql""" %
                (timeout))

    files = [f for f in os.listdir(query_dir) if f.endswith("sql")]
    files.sort()
    for num_run in xrange(num_runs):
        for filename in files:
            # Get query plan
            print "Getting plan for " + filename
            run_command("echo \"explain \" > __tmp_get_query_plan.sql && " +
                        "cat %s/%s >> __tmp_get_query_plan.sql >> __tmp_get_query_plan.sql" %
                        (query_dir, filename))
            run_command("presto-cli --catalog hive " +
                        "--schema %s --file __tmp_get_query_plan.sql > %s/plans/%s.plan" %
                        (databaseName, results_dir, filename))
            # Run the query
            print "Running " + filename
            run_command("""cat __tmp_presto_configs.sql %s/%s > __tmp_current_query.sql""" %
                        (query_dir, filename))
            run_command("presto-cli --catalog hive --schema %s --file __tmp_current_query.sql"
                        "> %s/runs/%s.run 2>&1" % (databaseName, results_dir, filename))
            # Get query runtime
            run_command("presto-cli --catalog hive --schema %s --file "
                        "__tmp_get_runtime_from_presto.sql >> %s/presto_runtimes_raw.csv" %
                        (databaseName, results_dir))

    run_command("rm -f __tmp*", True)


def convert_raw_results(raw_results):
    with open("%s/presto_results.csv" % (results_dir), "w") as results:
        results.write(
            'Name,Runtime,Vendor,System,Cluster,Configuration,Database,Date,Scale,Error\n')
        for line in open("%s/presto_runtimes_raw.csv" % (results_dir)):
            name, time, status = line.split(",")
            # Strip quotes
            name = name[1:-1]
            time = time[1:-1]
            status = status[1:-1]
            if status == "FAILED":
                time = "0"
            elif status == "FINISHED":
                status = ""  # just blank for ok.
            runtime = float(time) / 1000.0
            results.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %
                          (name, runtime, vendor, system, cluster, configuration,
                           databaseName, date, scaleFactor, status))


if __name__ == '__main__':
    run_presto_benchmark(query_dir, results_dir, num_runs)
    convert_raw_results("%s/presto_runtimes_raw.csv" % (results_dir))
