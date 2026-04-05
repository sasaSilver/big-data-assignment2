#!/bin/bash

IN=${1:-/input/data}
OUT="/indexer/index"

echo "Creating index: $IN -> $OUT..."
hdfs dfs -rm -r -f $OUT

mapred streaming \
    -input $IN \
    -output $OUT \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py

echo "Creating index complete"
