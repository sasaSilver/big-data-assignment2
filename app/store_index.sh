#!/bin/bash

source .venv/bin/activate
rm -f index.txt

echo "Storing index..."

hdfs dfs -getmerge /indexer/index index.txt

# Run the app to update the index
python3 app.py

echo "Index stored"
