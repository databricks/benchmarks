Scripts for running TPC-DS on Apache Spark on AWS
=================================================

This directory contains the scripts we use for producing TPC-DS results on Apache Spark on AWS, using EMR.

Includes:
  1. Bash script for setting up an EMR cluster.
  2. Non-default Spark configurations, which adds additional tuning for the cluster.
  3. Scala script providing convenience functions on top of the [spark-sql-perf](https://github.com/databricks/spark-sql-perf) API.

Usage
-----
Run the Bash script without any arguments to see how it's supposed to be used.
Then follow the instructions provided by the script itself.

Note that you'll have to supply:
  * An assembly jar of [spark-sql-perf](https://github.com/databricks/spark-sql-perf). Just check out the repo and run <code> ./build/sbt assembly </code>.
  * An EC2 Key Pair. To obtain one, go to the Amazon Web Console -> EC2 -> Key Pairs -> Create Key Pair.
  * An S3 bucket containing pre-generated TPC-DS data for the desired scale factors.
