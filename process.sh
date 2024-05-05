#!/bin/bash

set -euo pipefail
# -e and -o pipefail will make the script exit
#    in case of command failure (or piped command failure)
# -u will exit in case a variable is undefined
#    (in you case, if the header is invalid)

if [ $# -eq 0 ] || [ -z "$1" ]
  then
    DATASET_URLS='dataset_urls0.csv'
    echo "No arguments supplied - processing $DATASET_URLS"
else
  DATASET_URLS=$1
fi

DATA_DIRECTORY='data'
SCRIPT='process_jsonl.py'
RESET='false'
MODE='parquet'

POSTGRES_DBNAME=
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_HOST=
POSTGRES_PORT=

if [[ -e ".env" ]]
  then
    # loading script parameters from .env
    set -a            
    source .env
    set +a
    if [ -z "$POSTGRES_HOST" ] || [ -z "$POSTGRES_PORT" ] || [ -z "$POSTGRES_DBNAME" ] || [ -z "$POSTGRES_USER" ] || [ -z "$POSTGRES_PASSWORD" ] 
      then
        echo "Not all PostgreSQL paramaters set/loaded from .env. Exiting."
        exit 1
    fi   
else
    echo "No .env file with PostgreSQL paramaters found. Exiting."
    exit 1
fi

echo "PROCESSING: transforming & loading dataset files from '$DATASET_URLS' -> $MODE..."

# Preparing data directory
if [ ! -d "$DATA_DIRECTORY" ]; then
  echo " No '$DATA_DIRECTORY' directory found. Exiting."
  exit 1; 
fi

# Ingesting data files
while IFS=, read -r table archive url || [ -n "$table" ]; do
  # echo $table, $archive, $url

  # checking dataset file names
  if ! [[ "$archive" =~ .*\.jsonl.gz$ ]] || ! [[ "$url" =~ .*\.jsonl.gz$ ]] ; then
    # first line of .csv - header
    # not a valid (archive url) pair - skipping
    # echo " Unexpected data file $archive. Skipping."
    continue
  fi

  export filename="${archive%.gz}"
  if [[ "$filename" =~ .*\.jsonl$ ]] && [[ -e "$DATA_DIRECTORY/$filename" ]]; then
    # unpacked file found - processing
    echo " Processing $DATA_DIRECTORY/$filename ..."
    python $SCRIPT --user $POSTGRES_USER --password $POSTGRES_PASSWORD --host $POSTGRES_HOST --port $POSTGRES_PORT --db $POSTGRES_DBNAME --table_name $table --reset $RESET --source $DATA_DIRECTORY/$filename --mode $MODE
    if [[ $? -ne 0 ]]; then
      # Aborted - stopping
      echo " Processing stopped."
      exit 1
    fi  
    # processed - next
    continue
  fi 
   
  # unpacked file not found - checking archive
  if [[ "$archive" =~ .*\.jsonl.gz$ ]] && [[ -e "$DATA_DIRECTORY/$archive" ]]; then
    echo " Archive $DATA_DIRECTORY/$archive found. Testing..."
    # should I check archive?
    gzip -t $DATA_DIRECTORY/$archive
  fi  
  if [[ $? -ne 0 ]]; then
    echo " Testing $DATA_DIRECTORY/$archive failed. Deleting to re-download. Processing stopped."
    rm -rf $DATA_DIRECTORY/$archive
    exit 1
  fi  

  # archive found - processing
  if [[ "$archive" =~ .*\.jsonl.gz$ ]] && [[ -e "$DATA_DIRECTORY/$archive" ]]; then
    echo " Processing $DATA_DIRECTORY/$archive ..."
    python $SCRIPT --user $POSTGRES_USER --password $POSTGRES_PASSWORD --host $POSTGRES_HOST --port $POSTGRES_PORT --db $POSTGRES_DBNAME --table_name $table --reset $RESET --source $DATA_DIRECTORY/$archive --mode $MODE
    if [[ $? -ne 0 ]]; then
      # Aborted - stopping
      echo " Processing stopped."
      exit 1
    fi  
    # processed - next
    continue
  fi

done < $DATA_DIRECTORY/$DATASET_URLS

echo 'PROCESSING finished: OK!'
