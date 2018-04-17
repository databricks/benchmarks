Benchmarks comparing Pandas and PySpark
========================================

Repository for scripts to run benchmarks comparing Pandas and PySpark on single node machine.

## Pandas benchmark
```
usage: pandas_benchmark.py [-h] path

Pandas Query Benchmark

positional arguments:
  path        Path of TPC-DS Parquet file

optional arguments:
  -h, --help  show this help message and exit

E.g. python pandas_benchmark.py /data/tpcds250/store_sales/
```

## PySpark benchmark
```
usage: spark-submit pyspark_benchmark.py [-h] path

PySpark Query Benchmark

positional arguments:
  path        Path of TPC-DS Parquet file

optional arguments:
  -h, --help  show this help message and exit

E.g. spark-submit  --properties-file conf/spark-defaults.conf --master local[16] \
       pyspark_benchmark.py /data/tpcds250/store_sales
```
