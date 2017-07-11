/*
Copyright 2017 Databricks, Inc.

This work (the "Licensed Material") is licensed under the Creative Commons
Attribution-NonCommercial-NoDerivatives 4.0 International License. You may
not use this file except in compliance with the License.

To view a copy of this license, visit https://creativecommons.org/licenses/by-nc-nd/4.0/

Unless required by applicable law or agreed to in writing, the Licensed Material is offered
on an "AS-IS" and "AS-AVAILABLE" BASIS, WITHOUT REPRESENTATIONS OR WARRANTIES OF ANY KIND,
whether express, implied, statutory, or other. This includes, without limitation, warranties
of title, merchantability, fitness for a particular purpose, non-infringement, absence of
latent or other defects, accuracy, or the presence or absence of errors, whether or not known
or discoverable. To the extent legally permissible, in no event will the Licensor be liable
to You on any legal theory (including, without limitation, negligence) or otherwise for
any direct, special, indirect, incidental, consequential, punitive, exemplary, or other
losses, costs, expenses, or damages arising out of this License or use of the Licensed
Material, even if the Licensor has been advised of the possibility of such losses, costs,
expenses, or damages.
*/

import com.databricks.spark.sql.perf.tpcds.Tables
import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.Benchmark.ExperimentStatus

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, substring}


/** Full AWS EMR cluster description */
val clusterDescDF: DataFrame = {
  spark.read.json("cluster-desc.json")
}

/** Short AWS EMR cluster description to be recorded alongside test results */
val clusterShortDesc: String = {
  clusterDescDF
    .select(explode($"Cluster.InstanceGroups"))
    .select("col.Name", "col.RequestedInstanceCount", "col.InstanceType")
    .as[(String, Long, String)]
    .sort("Name")
    .collect.mkString
}

/** Schema name for the given scale factor */
def databaseName(scaleFactor: Int): String = {
  s"tpcds_sf${scaleFactor}"
}

/**
 * Executes DDLs for setting up external tables over existing data under `dataPath`.
 * @param dataPath location of already-generated TPC-DS data
 * @param scaleFactor should be the same as what was used when generating the data
 * @param format the format of the provided data - typically "parquet"
 * @param useDecimal whether or not the Decimal data type was used as opposed to Double
 * @param useDate whether or not the Date data type was used as opposed to String
 * @param analyzeTables whether or not ANALYZE TABLE commands should be run on all tables
 */
def create_external_tables(
    dataPath: String,
    scaleFactor: Int,
    format: String = "parquet",
    useDecimal: Boolean = false,
    useDate: Boolean = false,
    analyzeTables: Boolean = true)
  : Unit = {

  val database = databaseName(scaleFactor)

  sql(s"drop database if exists $database cascade")
  sql(s"create database $database")
  sql(s"use $database")

  val tables = new Tables(spark.sqlContext, dsdgenDir = "", scaleFactor,
    useDoubleForDecimal = !useDecimal, useStringForDate = !useDate)
  tables.createExternalTables(dataPath, format, database, overwrite = true, discoverPartitions = true)

  if (analyzeTables) {
    tables.analyzeTables(database, analyzeColumns = true)
  }
}

/**
 * Starts the experiment in a background thread and returns an [[ExperimentStatus]] handle.
 * Using this handle, one can access and export live results while the benchmark is still running.
 * It can be also waited on until it finishes. See `run_queries()` for an example.
 */
def start_queries(
    scaleFactor: Int,
    numIter: Int = 2,
    queryFilter: Seq[String] = Seq(),
    configuration: String = "std")
  : ExperimentStatus = {

  val database = databaseName(scaleFactor)
  sql(s"use $database")

  val tpcds = new TPCDS(spark.sqlContext)

  val queries = queryFilter match {
    case Seq() => tpcds.tpcds2_4Queries
    case _ => tpcds.tpcds2_4Queries.filter(q => queryFilter.contains(q.name))
  }

  val experiment = tpcds.runExperiment(
    queries,
    iterations = numIter,
    tags = Map(
      "runtype" -> "emr_benchmark",
      "date" -> java.time.LocalDate.now.toString,
      "database" -> database,
      "sf" -> scaleFactor.toString,
      "system" -> "Spark",
      "cluster" -> clusterShortDesc,
      "configuration" -> configuration,
      "spark_version" -> spark.version
      )
    )

  experiment
}

/** Exports the current results of `experiment` to the specified location in a standard format. */
def export_current_results(
    experiment: ExperimentStatus,
    resultsLocation: String)
  : DataFrame = {

  if (experiment.currentResults.size > 0) {
    assert(experiment.currentRuns.size > 0)
    val tags = experiment.currentRuns.head.tags
    experiment.getCurrentResults
      .withColumn("Name", substring(col("name"), 2, 100))
      .withColumn("Runtime",
        (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime")
          + col("executionTime")) / 1000.0)
      .select("Name", "Runtime")
      .withColumn("Vendor", lit("EMR"))
      .withColumn("Date", lit(tags("date")))
      .withColumn("Database", lit(tags("database")))
      .withColumn("Scale", lit(tags("sf")))
      .withColumn("System", lit(tags("system") + s" ${spark.version}"))
      .withColumn("Cluster", lit(tags("cluster")))
      .withColumn("Configuration", lit(tags("configuration")))
  } else {
    spark.emptyDataFrame
  }
}

/**
 * Executes a full TPC-DS run and exports final results to the specified location.
 * @param scaleFactor
 * @param numIter number of iterations
 * @param queryFilter list of queries to run, or Seq.empty for "all"
 * @param configuration free-form tag of the run - e.g. "std", "cbo", "myconfig1", etc.
 * @param timeout maximum time (in seconds) to wait for an experiment to finish
 * @param resultsLocation path where the experiment results should be written / appended
 */
def run_queries(
    scaleFactor: Int,
    numIter: Int = 2,
    queryFilter: Seq[String] = Seq(),
    configuration: String = "std",
    timeout: Int = 36 * 60 * 60,
    resultsLocation: String = "results")
  : Unit = {

  val experiment = start_queries(scaleFactor, numIter, queryFilter, configuration)
  print(experiment)

  experiment.waitForFinish(timeout)

  export_current_results(experiment, resultsLocation)
    .coalesce(1)
    .write
    .format("csv")
    .mode("append")
    .save(resultsLocation)
}

/** Runs a full experiment with all default parameters. */
def run_default_test(dataPath: String, scaleFactor: Int): Unit = {
  create_external_tables(dataPath, scaleFactor)
  run_queries(scaleFactor)
}


