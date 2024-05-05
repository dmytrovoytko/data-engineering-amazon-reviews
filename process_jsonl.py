import os
import argparse
from pathlib import Path
from time import time, strftime

import pandas as pd
# pd.set_option('display.max_columns', None)

from sqlalchemy import create_engine

from export_bigquery import export_parquet_to_bigquery

# Processing mode
PARQUET = 'parquet' # export to .parquet files
POSTGRES = 'postgres' # load to PostgreSQL
SAMPLE = 'sample' # export samples to .csv 

# defaults
SAMPLE_SIZE = 100
CHUNKSIZE = 50_000
MODE = POSTGRES # or PARQUET or SAMPLE
DEBUG = False # True

def verbose(df):
    if DEBUG:
        return df.head().to_string(index=False)
    else:
        return ''

def check_file_name(source, table_name):
    # this script works with plain or packed data files - .jsonl.gz / .jsonl
    if not source.endswith('.jsonl') and not source.endswith('.jsonl.gz'):
        print('Source file must be .jsonl or .jsonl.gz. Ingestion stopped.')
        return ''

    if not table_name in ['meta', 'books', 'reviews']:
        print('Please check table_name, should it be "meta"/"books" or "reviews" Aborting.')
        return ''

    # extra check of filename and table_name consistency
    file_name = Path(source).name # without full path
    if file_name.startswith('meta_') and (not table_name in ['meta', 'books']): 
        # products file?
        print('Please check table_name, should it be "meta"/"books"? Aborting.')
        return ''
    elif (not file_name.startswith('meta_')) and (table_name in ['meta', 'books']): 
        # reviews file?
        print('Please check table_name, should it be "reviews"? Aborting.')
        return ''

    return file_name

def check_params(mode, params):
    if mode==POSTGRES and not (params.host and params.port and params.db and params.user and params.password):
        print('Error: the following arguments are required for postgres ingestion: --host, --port, --db, --user, --password')
        return False
    return True

def reset_table(engine, table_name, df):
    print(f'Table creation/reset {table_name}, columns: {df.columns.tolist()}\n{df.dtypes.to_string()}\n')
    try:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        return True
    except Exception as e:
        print('Table creation/reset failed. Ingestion stopped.\n',e)
        return False

def get_category_transformation(table_name, file_name):
    if table_name=='meta': 
        # products file
        if 'Software' in file_name:
            # Some products have incorrect main_category - should be replaced to 'Software'
            # ! Some products have empty main_category (null) - should be replaced to 'Software'
            category_transformation = {'Software': 
                ['AMAZON FASHION', 'Books', 'Computers', 'Gift Cards', 'Home Audio & Theater', 'Toys & Games', 'None']}
        elif 'Magazine_Subscriptions' in file_name:
            # Some products have incorrect main_category - should be replaced 
            # ! Some products have empty main_category (null) - should be replaced
            category_transformation = {'Magazine Subscriptions': 
                ['Books', 'None']}
        elif 'Video_Games' in file_name:
            # Some products have incorrect main_category - should be replaced
            # ! Some products have empty main_category (null) - should be replaced
            # TODO oh. it's a mess there!
            category_transformation = {'Video Games': 
                ['Amazon Devices', 'Appliances', 'Audible Audiobooks', 'Baby', 'Car Electronics', 'Collectible Coins', 'Gift Cards', 'GPS & Navigation', 'Grocery', 'Handmade', '', 'None']}
        else:
            category_transformation = {}            
    elif table_name=='books':
        if 'Kindle_Store' in file_name:
            # Some products have incorrect main_category - should be replaced 
            # ! Some products have empty main_category (null) - should be replaced
            category_transformation = {'Kindle Store': 
                ['Buy a Kindle', 'Software', 'Magazine Subscriptions', '', 'None']}
    else: 
        # reviews file
        category_transformation = {}
    return category_transformation

def get_selected_columns(table_name):
    if table_name=='meta': 
        # products file
        selected_columns = ['parent_asin','title','main_category','average_rating','rating_number']
    elif table_name=='books': 
        # products file
        selected_columns = ['parent_asin','title','main_category','average_rating','rating_number']
    else: 
        # reviews file
        selected_columns = ['parent_asin','verified_purchase', 'rating', 'helpful_vote', 'user_id', 
                            ## 'timestamp', - renamed!
                            'review_date',
                            ]
    return selected_columns

def get_extra_detail_columns(table_name, file_name):
    return {}

def get_dtypes(table_name):
    # For be sure data file is structured as planned, and regulate pandas memory consumption
    # it is recommended to define dtypes, comments on the correspondence:
        # Some NumPy dtypes don't exist in built-in Python, so require quotes.
        # Valid dtype options:
        #     Built-in python dtypes: int, float, str, bool, object.
        #     NumPy dtypes(as strings): 'int8', 'int16', 'int32', 'int64', 'float16', 'float32', 'float64', 'string_', 'category', 'datetime64[ns]'.       
        # Correct way: dtype={'column_name': 'Int64'}

    if table_name in ['meta', 'books']: 
        # products file

        # Source file structure
        dtypes = {
                'main_category': str, # Main category (i.e., domain) of the product.
                'title': str, # Name of the product.
                'average_rating': 'float32', # Rating of the product shown on the product page.
                'rating_number': 'int32', # Number of ratings in the product.
                'features': object, # list, Bullet-point format features of the product.
                'description': object, # list, Description of the product.
                'price': float, # Price in US dollars (at time of crawling).
                'images': object, # list, Images of the product. Each image has different sizes (thumb, large, hi_res). The “variant” field shows the position of image.
                'videos': object, # list, Videos of the product including title and url.
                'store': str, # Store name of the product.
                'categories': object, # list, Hierarchical categories of the product.
                'details': dict, # Product details, including materials, brand, sizes, etc.
                'parent_asin': 'category', # Parent ID of the product.
                'bought_together': object # list', Recommended bundles from the websites. 
                }
    else: 
        # reviews file
        # Source file structure
        dtypes = {
                'rating': 'float32', # Rating of the product (from 1.0 to 5.0).
                'title': str, # Title of the user review.
                'text': str, # Text body of the user review.
                'images': object, # list, Images that users post after they have received the product. Each image has different sizes (small, medium, large), represented by the small_image_url, medium_image_url, and large_image_url respectively.
                'asin': str, # ID of the product.
                'parent_asin': 'category', # Parent ID of the product. Note: Products with different colors, styles, sizes usually belong to the same parent ID. The “asin” in previous Amazon datasets is actually parent ID. Please use parent ID to find product meta.
                'user_id': 'category', # ID of the reviewer
                'timestamp': 'datetime64[ns]', # Time of the review (unix time)
                'verified_purchase': bool, # User purchase verification
                'helpful_vote': 'int32', # Helpful votes of the review
                }
    return dtypes

def preprocess_meta(df, chunk):
    # replacing rating_number = null with 0, then fixing wrong dtype (float because of nulls)
    try:
        df_repl = df[df['average_rating'].isnull()]
        if df_repl.shape[0]:
            print(f'Replacing null in average_rating -> 3: {df_repl.shape[0]} record(s)')
            df.loc[chunk['average_rating'].isnull(), 'average_rating'] = 3
        df_repl = df[df['rating_number'].isnull()]
        if df_repl.shape[0]:
            print(f'Replacing null in rating_number -> 0: {df_repl.shape[0]} record(s)')
            df.loc[chunk['rating_number'].isnull(), 'rating_number'] = 0
        # fixing wrong dtype    
        df['rating_number'] = df['rating_number'].astype('int32')

        # checks, no filtering, just warnings
        df_repl = df[df['parent_asin'].astype(str).map(len)==0]
        if df_repl.shape[0]:
            print(f'! Empty parent_asin: {df_repl.shape[0]} record(s)\n{verbose(df_repl)}')

        df_repl = df[df['title'].astype(str).map(len)==0]
        if df_repl.shape[0]:
            print(f'! Empty title: {df_repl.shape[0]} record(s)\n{verbose(df_repl)}')

    except Exception as e:
        print('Replacing null / fixing type error', e)
        pass
    return df

def transform_meta(df, category_transformation):
    # incorrect main_category - should be replaced (ex. Software) 
    for replacement in category_transformation:
        for category in category_transformation[replacement]:
            df_repl = df[df['main_category']==category]
            if df_repl.shape[0]:
                print(f'Replacing incorrect main_category: {category} -> {replacement}, {df_repl.shape[0]} record(s)\n{verbose(df_repl)}')
                df.loc[df['main_category']==category, 'main_category'] = replacement

        # null 
        # fill empty main_category
        df_repl = df[df['main_category']=='None']
        if df_repl.shape[0]:
            print(f'Replacing null in main_category -> {replacement}: {df_repl.shape[0]} record(s)')
            df.loc[df['main_category']=='None', 'main_category'] = replacement

        # replacing rating_number = null with 0
        try:
            df_repl = df[df['rating_number'].isnull()]
            if df_repl.shape[0]:
                print(f'Replacing null in rating_number -> 0: {df_repl.shape[0]} record(s)')
                df.loc[df['rating_number'].isnull(), 'rating_number'] = 0
            df_repl = df[df['rating_number']<0]
            if df_repl.shape[0]:
                print(f'Replacing negative numbers in rating_number -> 0: {df_repl.shape[0]} record(s)')
                df.loc[df['rating_number']<0, 'rating_number'] = 0
        except Exception as e:
            print('Replacing null in rating_number error', e)
            pass

    return df

def extract_kindle_meta(df, selected_details):
    return df

def export_data_to_parquet(df, file_name, file_name_list):
    df.to_parquet(file_name, index=False, engine='pyarrow')
    file_name_list.append(file_name)
    return file_name_list

def main(params):
    jsonl_name = params.source
    table_name = params.table_name
    mode = params.mode
    host = params.host 
    port = params.port 
    db = params.db
    user = params.user
    password = params.password
    reset = params.reset
    chunksize = params.chunksize
    
    file_name = check_file_name(jsonl_name, table_name)
    if file_name=='':
        return 1

    if not check_params(mode, params):
        return 1 

    if mode==POSTGRES:
        print(f'\nConnecting to PostgreSQL: {host}:{port}/{db}...')
        try:
            engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        except Exception as e:
            print('PostgreSQL connection failed. Ingestion stopped.\n', e)
            return 1
    else:
        print(f' Mode: {mode}')

    t_start = time()
    t_start0 = t_start
    print(f'\n[{strftime("%H:%M:%S")}] Loading {jsonl_name}')

    # for reducing memory consumption by setting explicit dtypes 
    dtypes = get_dtypes(table_name)
    # for reducing unnecessary data from original file to process selected columns only 
    selected_columns = get_selected_columns(table_name)
    # for extracting some data from 'details' column 
    extra_columns = get_extra_detail_columns(table_name, file_name)
    # final columns in desired order + correct ids
    # final_columns = selected_columns + [item.lower().replace(' ', '_') for item in extra_columns]
    final_columns = selected_columns + [val for key,val in extra_columns.items()]
    if table_name=='books': 
        final_columns.remove('details')
    # for fixing incorrect main_category
    category_transformation = get_category_transformation(table_name, file_name)

    i = 0
    export_list = []
    # read_json is smart to process jsonl or autodetect .gz archived file by extension
    chunks = pd.read_json(jsonl_name, lines=True, chunksize=chunksize, dtype=dtypes)
    for chunk in chunks:
        i += 1

        try:
            # print(type(chunk))
            df = chunk
            # print(f'Chunk {i}...')
        except Exception as e:
            print(e)
            return 1

        # PRE PROCESSING
        if table_name in ['meta', 'books']: # products file
            df = preprocess_meta(df, chunk)
            # if table_name=='books':
            #     # extracting key details for books
            #     df = extract_kindle_meta(df, selected_columns)

        # checks ? no serious reasons, performance is more important for now
        # duplicate parent_asin in meta? - datasets are correct on this, and not critical
        # check are there any not unique timestamps? - it's possible, and not critical 
        # duplicate titles? - many are empty, some duplicate, not critical

        # transformation #1 reviews, (!) before reducing to [selected_columns]
        if table_name=='reviews':
            # Renaming 'timestamp' column as it is a reserved keyword in many systems 
            # - to prevent unexpected errors
            df.columns = df.columns.str.replace('timestamp', 'review_date')
            df['review_date'] = df['review_date'].astype('datetime64[s]')

        if i==1 and mode==SAMPLE:
            # export sample 0: before final transformation 
            # print(df.head(5))
            df.head(SAMPLE_SIZE).to_csv(jsonl_name+'_0.csv', encoding='utf-8', index=False)

        # transformation #2
        # fixing null values, incorrect main_category
        if table_name=='meta': # products file
            df = transform_meta(df, category_transformation)
        elif table_name=='books': # products file
            # extracting key details for books
            df = extract_kindle_meta(df, extra_columns)
            df = transform_meta(df, category_transformation)
            df = rename_columns(df)

        # transformation #3
        # reducing data to the columns we need for project + reordering them for more convenient analysis
        try:
            # all_columns = df.columns.values.tolist()
            # columns_to_drop = [item for item in all_columns if item not in selected_columns]
            # df = df.drop(columns=columns_to_drop) # dropping creates copy - longer
            df = df[final_columns]
        except Exception as e:
            print(f'Reducing to {final_columns} failed. Aborting.\n{e}')
            return 1

        # if table_name=='books': 
        #     df = df.drop(columns='details')

        if i==1 and mode==SAMPLE:
            # export sample 1: after final transformation 
            df.head(SAMPLE_SIZE).to_csv(jsonl_name+'_1.csv', encoding='utf-8', index=False)
            # no full export - exiting
            print(f'Finished exporting {jsonl_name} -> samples. Total time {(time() - t_start0):.3f} second(s)\n+++\n')
            return 0

        if mode == PARQUET:
            export_data_to_parquet(df, f'{jsonl_name}-{i:02d}.parquet', export_list)
            print(f'... {jsonl_name}-{i:02d}.parquet, {df.shape[0]} record(s), took {(time() - t_start):.3f} second(s)')
            t_start = time()
            continue

        # mode == POSTGRES
        # replacing the table if it's the first chunk & reset param set True
        if i==1 and reset and reset.lower()=='true':
            res = reset_table(engine, table_name, df)
            if res == False:
                return 1

        # Exporting to PostgreSQL 
        try:
            df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
        except Exception as e:
            print('Appending chunk {i} to PostgreSQL table failed. Ingestion stopped.\n',e)
            print(df.head())
            return 1

        print(f'... chunk {i:02d} appended, {df.shape[0]} record(s), took {(time() - t_start):.3f} second(s)')
        t_start = time()

    if mode == PARQUET:
        # TODO ? saving export_list to file
        print(f'Finished exporting {jsonl_name} to parquet files. Total time {(time() - t_start0):.3f} second(s)\n+++\n')
        export_parquet_to_bigquery(table_name, export_list, file_name)
        print(f'Finished ingesting {jsonl_name} into BigQuery. Total time {(time() - t_start0):.3f} second(s)\n+++\n')
    else:
        print(f'Finished ingesting {jsonl_name} into the PostgreSQL database. Total time {(time() - t_start0):.3f} second(s)\n+++\n')

    return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest jsonl data to Postgres or export sample/parquet') # TODO Process?

    parser.add_argument('--source', required=True, help='source jsonl[.gz] file to process')
    parser.add_argument('--table_name', required=True, help='name of the table to load data')
    parser.add_argument('--mode', required=False, type=str, default=POSTGRES, help=f'{POSTGRES}/{SAMPLE}/{PARQUET}, {POSTGRES} as default')
    parser.add_argument('--host', required=False, help='host for postgres')
    parser.add_argument('--port', required=False, help='port for postgres')
    parser.add_argument('--db', required=False, help='database name for postgres')
    parser.add_argument('--user', required=False, help='user name for postgres')
    parser.add_argument('--password', required=False, help='password for postgres')
    parser.add_argument('--reset', required=False, type=str, default='False', help='True to reset table before loading, False as default')
    parser.add_argument('--chunksize', required=False, type=int, default=CHUNKSIZE, help=f'processing chunk size, {CHUNKSIZE} as default')

    args = parser.parse_args()

    res = main(args)
    exit(res)
