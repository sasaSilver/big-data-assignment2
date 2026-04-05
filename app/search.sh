#!/bin/bash

source .venv/bin/activate

if [ ! -f ".venv.tar.gz" ]; then
    venv-pack -o .venv.tar.gz
fi

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit --master yarn --archives .venv.tar.gz#.venv query.py "$1"
