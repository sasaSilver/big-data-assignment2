#!/bin/bash

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

hdfs dfs -rm -r -f /input/data /data /e.parquet

echo "Preparing data..."

hdfs dfs -put -f e.parquet /
spark-submit prepare_data.py
hdfs dfs -mkdir -p /data
hdfs dfs -put -f data/*.txt /data/

echo "Preparing data complete"
