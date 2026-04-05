#!/bin/bash
set -euo pipefail

echo "Indexing..."

bash create_index.sh ${1:-/input/data}
bash store_index.sh

echo "Indexing complete"
