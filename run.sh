#!/bin/bash

. ~/.pgvars
COLLECTL_PATH=/home/msi/chilton/workspace/collectl/collectl.pl
HOSTS="elmod,elmo,koronis"
RAW_DIRECTORY=/project/collectl
NUM_THREADS=8

. .venv/bin/activate
python lib/parse_collectl.py  --num_threads="$NUM_THREADS" --collectl_path="$COLLECTL_PATH" --hosts="$HOSTS" --directory="$RAW_DIRECTORY"