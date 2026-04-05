#!/bin/bash

# Start ssh server
service ssh restart
bash start-services.sh

# Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

# Package the venv
venv-pack -o .venv.tar.gz

echo "STEP 1: DATA PREPARATION"
bash prepare_data.sh

echo "STEP 2: INDEXING"
bash index.sh

echo "STEP 3: SEARCH"

echo "SEARCH 1: economy"
bash search.sh "economy"

# echo "SEARCH 2: elizabeth artist"
# bash search.sh "elizabeth artist"

echo "ALL STEPS COMPLETE"
sleep infinity
