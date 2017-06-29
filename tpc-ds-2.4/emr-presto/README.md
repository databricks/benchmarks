Scripts for running TPC-DS on Presto
====================================

This directory contains the scripts we use for producing TPC-DS results on Presto, using EMR.
The scripts are a bit rough, use with caution!

Includes:
  1. Set of TPCDS queries that can be ran on Presto, based on TPCDS v2.4 queries from spark-sql-perf.
    * 3 queries (26, 70, 86) are disabled, because they use unsupported "grouping" in rollup.
    * Several queries have been rewritten to have explicit casts between string and date, that didn't work automatically in Presto.
    * Several queries have been rewritten to use double-quotes (") instead of backticks (`) for alias names.
    * No more advanced rewrites have been made.
  2. Python script providing convenience functions to run the queries.

Usage
-----

1. To create an EMR cluster, and then set up TPCDS schema in Hive catalog, refer to the scripts from `../emr-spark`.
2. Upload the script and queries folder to the cluster.
3. At the top of the script, there are several hardcoded global variables that may need to be modified, they are documented inline.

Results of a run will be saved in `results/timestamp=NNN`.
