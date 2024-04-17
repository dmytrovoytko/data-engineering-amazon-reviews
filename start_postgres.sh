# starting PostgreSQL in docker
# data will stay in folder ./amzn_volume so you can run it again without need to ingest data from scratch 
#--network=bridge 

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

docker run -it -e POSTGRES_USER=$POSTGRES_USER -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -e POSTGRES_DB=$POSTGRES_DBNAME -v "./amzn_volume:/var/lib/postgresql/data" -p $POSTGRES_PORT:5432 postgres

# check connection
# pass: postg
# pgcli -h 172.17.0.2 -p 5432 -u postg -d amzn_reviews