#!/bin/bash

. ~/.pgvars
COLLECTL_PATH=/home/softacct/collectl/collectl.pl
#HOSTS="elmod,elmo,koronis"
HOSTS="koronis,elmod,calhoun"
RAW_DIRECTORY=/project/collectl

. .venv/bin/activate
python lib/parse_collectl.py --remote_host="loon" --remote_user="chilton" --collectl_path="$COLLECTL_PATH" --hosts="$HOSTS" --directory="$RAW_DIRECTORY" --from=20120701
