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

if [[ -e ".env" ]]
  then
    # loading script parameters from .env
    set -a            
    source .env
    set +a
    if [ -z "$DATA_DIRECTORY" ]
      then
        DATA_DIRECTORY='data'
    fi   
fi

# If you want to save wget log - uncomment 2 lines below + wget option 
# DOWNLOAD_LOG='download_log.txt'
# > $DOWNLOAD_LOG

echo "EXTRACTION: downloading dataset files from '$DATASET_URLS' to '$DATA_DIRECTORY'..."

# Preparing data DATA_DIRECTORY
if [ ! -d "$DATA_DIRECTORY" ]; then
  echo " Creating DATA_DIRECTORY '$DATA_DIRECTORY'."
  mkdir $DATA_DIRECTORY
  if [[ $? -ne 0 ]]; then
    echo " Creating '$DATA_DIRECTORY' DATA_DIRECTORY failed. Exiting."
    exit 1; 
  fi
fi

# Downloading & unpacking dataset files
while IFS=, read -r table archive url || [ -n "$table" ]; do
  # echo $table, $archive, $url

  # checking 
  if ! [[ "$archive" =~ .*\.jsonl.gz$ ]] || ! [[ "$url" =~ .*\.jsonl.gz$ ]] ; then
    # first line of .csv - header
    # not a valid (archive url) pair - skipping
    continue
  fi

  export filename="${archive%.gz}"
  if [[ "$filename" =~ .*\.jsonl$ ]] && [[ -e "$DATA_DIRECTORY/$filename" ]]; then
    echo " File $DATA_DIRECTORY/$filename exists."
    continue
  fi 
   
  if [[ "$archive" =~ .*\.jsonl.gz$ ]] && [[ -e "$DATA_DIRECTORY/$archive" ]]; then
    echo " Archive $DATA_DIRECTORY/$archive exists. Testing..."
    # should I check/Resume archive?
    gzip -t $DATA_DIRECTORY/$archive
  fi  
  if [[ $? -ne 0 ]]; then
    # should I ask to Resume downloading archive? 
    # for automation - no quiestions -> from scratch
    echo " Testing $DATA_DIRECTORY/$archive failed. Deleting to re-download."
    rm -rf $DATA_DIRECTORY/$archive
  fi  


  if [[ "$archive" =~ .*\.jsonl.gz$ ]] && ! [[ -e "$DATA_DIRECTORY/$archive" ]]; then
    echo " Downloading $archive to '$DATA_DIRECTORY'..."
    # to log wget output add option: -a $DOWNLOAD_LOG
    wget -O $DATA_DIRECTORY/$archive --no-check-certificate $url
    if [[ $? -ne 0 ]]; then
      echo " Downloading $archive to '$DATA_DIRECTORY' failed. Exiting."
      exit 1; 
    fi  
  fi

  continue 
  
  if [[ -e "$DATA_DIRECTORY/$archive" ]]; then
    echo " Extracting $DATA_DIRECTORY/$archive"
    gzip -d $DATA_DIRECTORY/$archive
    if [[ $? -ne 0 ]]; then
      echo " Extracting $archive to '$DATA_DIRECTORY' failed. Exiting."
      exit 1; 
    fi  
  fi    

done < $DATA_DIRECTORY/$DATASET_URLS

echo 'EXTRACTION finished: OK!'
ls --block-size=MB -1sk $DATA_DIRECTORY
