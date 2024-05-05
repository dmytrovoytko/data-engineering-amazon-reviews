import os
from glob import glob
from time import time

from google.cloud import bigquery
from google.api_core import exceptions
# from google.cloud import storage

# import pandas as pd

from dotenv import load_dotenv

def get_schema(table_name):
    if table_name=='meta':
        schema = [
            bigquery.SchemaField('parent_asin', 'STRING'),
            bigquery.SchemaField('title', 'STRING'),
            bigquery.SchemaField('main_category', 'STRING'),
            bigquery.SchemaField('average_rating', 'FLOAT'),
            bigquery.SchemaField('rating_number', 'INTEGER'),
            ]
    elif table_name=='books':
        schema = [
            ]
    else:
        schema = [
            # SchemaField('partition_date', 'DATE')
            bigquery.SchemaField('parent_asin', 'STRING'),
            bigquery.SchemaField('verified_purchase', 'BOOLEAN'),
            bigquery.SchemaField('rating', 'FLOAT'),
            bigquery.SchemaField('helpful_vote', 'INTEGER'),
            bigquery.SchemaField('user_id', 'STRING'),
            bigquery.SchemaField('review_date', 'TIMESTAMP'),
            ]
    return schema

def connect_bigquery(table_name):
    # loading environment variables, including Google Cloud credentials
    load_dotenv()

    if ('GOOGLE_APPLICATION_CREDENTIALS' in os.environ):
        CREDENTIALS = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        # print('GOOGLE_APPLICATION_CREDENTIALS:',CREDENTIALS)
        if (not os.path.exists(CREDENTIALS)):
            print (f'The GOOGLE_APPLICATION_CREDENTIALS file {CREDENTIALS} does not exist.\n')
            return None, None, None
    else:
        print ('The GOOGLE_APPLICATION_CREDENTIALS environment variable is not defined.\n')
        return None, None, None

    project_name = os.environ['GC_PROJECT_NAME']
    BQ_DATASET = os.environ['BQ_DATASET']
    BQ_TABLE = table_name
    schema = get_schema(BQ_TABLE)
    # bucket_name = os.environ['GCS_BUCKET']
    # storage_client = storage.Client()

    print(f'Ingestion to {project_name}.{BQ_DATASET}.{BQ_TABLE}')

    try:
        bq_client = bigquery.Client(project=project_name)
    except Exception as e:
        print (f'Error connecting to BigQuery project {project_name}.\n{e}')
        return None, None, None

    # Setup the BigQuery dataset object
    try:
        dataset_ref = bq_client.dataset(BQ_DATASET)
        dataset = bigquery.Dataset(dataset_ref)
        bq_client.get_dataset(dataset_ref)
        print(f' Found dataset {BQ_DATASET}')
    except exceptions.NotFound:
        # no such dataset found - creating it 
        try:
            dataset_ref = bq_client.dataset(BQ_DATASET)
            dataset = bq_client.create_dataset(bigquery.Dataset(dataset_ref))
            print(f' Created dataset {dataset.project}.{dataset.dataset_id}')
        except Exception as e:
            print (f'Error creating BigQuery dataset {BQ_DATASET}.\n{e}')
            return None, None, None

    try:
        table_ref = dataset.table(BQ_TABLE)
        table = bq_client.get_table(table_ref)
        print(f' Connected to table {table.project}.{table.dataset_id}.{table.table_id}')
    except:
        # no such table found - creating it 
        table_ref = dataset.table(BQ_TABLE)
        # define schema for parsing to loadconfig
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table)
        print(f' Created table {table.project}.{table.dataset_id}.{table.table_id}')

    return bq_client, table_ref, schema
    
def export_data_to_bigquery(bq_client, table_ref, parquet_files, schema, description):
    t_start0 = time()
    print(f'\nExporting {description} data to {table_ref}...')
    # print(f' Schema: {schema}\n')
    job_config = bigquery.LoadJobConfig(autodetect = True, 
                                        source_format = bigquery.SourceFormat.PARQUET,
                                        schema=schema,
                                        write_disposition='WRITE_APPEND',
                                        ignore_unknown_values = True,
                                        )

    for file_name_parquet in parquet_files: 
        if not file_name_parquet.endswith('parquet'):
            # TODO ? warning
            continue 
        with open(file_name_parquet, 'rb') as source_file:
            # # debug types 
            # df = pd.read_parquet(file_name_parquet, engine='pyarrow')
            # print(f'From parquet columns: {df.columns.tolist()}\n{df.dtypes.to_string()}\n')
            # print(f'{df.head(5).to_string()}\n')

            try:
                job = bq_client.load_table_from_file(
                    source_file, table_ref, job_config=job_config
                )
                result = job.result()

                # Get the number of rows loaded
                rows_loaded = result.output_rows
                print(f' + {rows_loaded} rows loaded to BigQuery {table_ref}.')
            except Exception as e:
                print(f'! Error while loading {file_name_parquet} to BigQuery {table_ref}.\n{e}')
                return 1
    print(f'\nData export {description} completed successfully. Took {(time() - t_start0):.3f} second(s)')
    return 0

def export_parquet_to_bigquery(table_name, file_list, description):

    print(f'Processing {description} {table_name} {file_list}')
    bq_client, table_ref, schema = connect_bigquery(table_name)
    if not bq_client:
        return 1

    export_data_to_bigquery(bq_client, table_ref, file_list, schema, description)

    t_start = time()
    table = bq_client.get_table(table_ref)
    # print(f'Table description: {table.description}')
    # print(f'Table schema: {table.schema}')
    print(f'Table has {table.num_rows} rows. Request took {(time() - t_start):.3f} second(s)')

    return 0  

if __name__ == '__main__':
    path = 'data'
    mask = 'meta_Digital_Music*.parquet' # dataset_urls0 export files
    try:
        file_list = [ sorted(glob(f'{path}/{mask}')) [0] ] # only 1st for testing
    except:
        print(f'No {path}/{mask} files found.')
        exit(1)
    description = f'Testing export {mask}'
    table_name = 'meta'
    # Export files to BigQuery
    export_parquet_to_bigquery(table_name, file_list, description)