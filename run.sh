#!/bin/bash

. ~/.pgvars
COLLECTL_PATH=/home/msi/chilton/workspace/collectl/collectl.pl
#HOSTS="elmod,elmo,koronis"
HOSTS="itasca"
RAW_DIRECTORY=/project/collectl
NUM_THREADS=4

. .venv/bin/activate
python lib/parse_collectl.py --collectl_path="$COLLECTL_PATH" --hosts="$HOSTS" --directory="$RAW_DIRECTORY" --from=20120301 --to=20120501
