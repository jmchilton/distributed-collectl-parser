#!/bin/bash

. ~/.pgvars

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$SCRIPT_DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

COLLECTL_PATH=${COLLECTL_PATH:-$SCRIPT_DIR/../collectl/collectl.pl}
HOSTS=${HOSTS:-"labqueue,BMSDL,BSCL,CGL,SDVL,elmod,cascade,itasca,calhoun"}
RAW_DIRECTORY=${RAW_DIRECTORY:-/project/collectl}

. .venv/bin/activate
#python lib/parse_collectl.py --remote_host="loon" --remote_user="chilton" --collectl_path="$COLLECTL_PATH" --hosts="$HOSTS" --directory="$RAW_DIRECTORY" --from=20120701
python lib/parse_collectl.py --collectl_path="$COLLECTL_PATH" --hosts="$HOSTS" --directory="$RAW_DIRECTORY" --from=20130101 "$@"
