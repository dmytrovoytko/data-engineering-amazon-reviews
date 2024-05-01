import os
import argparse
from pathlib import Path
from time import time, strftime
import json

import pandas as pd

from sqlalchemy import create_engine

CHUNKSIZE = 50_000
DEBUG = False # True
def verbose(df):
    if DEBUG:
        return df.head().to_string(index=False)
    else:
        return ''

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
            # oh. it's a mess there!
            category_transformation = {'Video Games': 
                ['Amazon Devices', 'Appliances', 'Audible Audiobooks', 'Baby', 'Car Electronics', 'Collectible Coins', 'Gift Cards', 'GPS & Navigation', 'Grocery', 'Handmade', '', 'None']}
        else:
            category_transformation = {}            
    else: 
        # reviews file
        category_transformation = {}
    return category_transformation

def get_selected_columns(table_name):
    if table_name=='meta': 
        # products file
        selected_columns = ['parent_asin','title','main_category','average_rating','rating_number']
    else: 
        # reviews file
        selected_columns = ['parent_asin','verified_purchase', 'rating', 'helpful_vote', 'user_id', 
                            ## 'timestamp', - renamed!
                            'review_date',
                            ]
    return selected_columns

def get_dtypes(table_name):
    # For be sure data file is structured as planned, and regulate pandas memory consumption
    # it is recommended to define dtypes, comments on the correspondence:
        # Some NumPy dtypes don't exist in built-in Python, so require quotes.
        # Valid dtype options:
        #     Built-in python dtypes: int, float, str, bool, object.
        #     NumPy dtypes(as strings): 'int8', 'int16', 'int32', 'int64', 'float16', 'float32', 'float64', 'string_', 'category', 'datetime64[ns]'.       
        # Correct way: dtype={'column_name': 'Int64'}

    if table_name=='meta': 
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

def check_file_name(source, table_name):
    # this script works with plain or packed data files - .jsonl.gz / .jsonl
    if not source.endswith('.jsonl') and not source.endswith('.jsonl.gz'):
        print('Source file must be .jsonl or .jsonl.gz. Ingestion stopped.')
        return ''

    if not table_name in ['meta', 'reviews']:
        print('Please check tablename, should it be "meta" or "reviews" Aborting.')
        return ''

    # extra check of filename and table_name consistency
    file_name = Path(source).name # without full path
    if file_name.startswith('meta_') and (not table_name=='meta'): 
        # products file?
        print('Please check tablename, should it be "meta"? Aborting.')
        return ''
    elif (not file_name.startswith('meta_')) and table_name=='meta': 
        # reviews file?
        print('Please check tablename, should it be "reviews"? Aborting.')
        return ''

    return file_name

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    jsonl_name = params.source
    reset = params.reset
    
    file_name = check_file_name(jsonl_name, table_name)
    if file_name=='':
        return 1

    print(f'\nConnecting to PostgreSQL: {host}:{port}/{db}...')
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    except Exception as e:
        print('PostgreSQL connection failed. Ingestion stopped.\n', e)
        return 1

    t_start = time()
    t_start0 = t_start
    print(f'[{strftime("%H:%M:%S")}] Loading {jsonl_name}')

    # reducing memory consumption by setting explicit dtypes 
    dtypes = get_dtypes(table_name)
    # reducing unnecessary data from original file to process selected columns only 
    selected_columns = get_selected_columns(table_name)
    # for fixing incorrect main_category
    category_transformation = get_category_transformation(table_name, file_name)

    i = 0
    # read_json is smart to process jsonl or autodetect .gz archived file by extension
    chunks = pd.read_json(jsonl_name, lines=True, chunksize=CHUNKSIZE, dtype=dtypes)
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
        if table_name=='meta': # products file
            df = preprocess_meta(df, chunk)

        # checks ? no serious reasons, performance is more important for now
        # duplicate parent_asin in meta? - datasets are correct on this, and not critical
        # check are there any not unique timestamps? - it's possible, and not critical 
        # duplicate titles? - many are empty, some duplicate, not critical

        # transformation #1 reviews, !before [selected_columns]
        if table_name=='reviews':
            # Renaming 'timestamp' column as it is a reserved keyword in many systems 
            # - to prevent unexpected errors
            df.columns = df.columns.str.replace('timestamp', 'review_date')

        # transformation #2
        # reducing data to the columns we need for project
        try:
            df = df[selected_columns]
        except Exception as e:
            print(f'Reducing to {selected_columns} failed. Aborting.\n{e}')
            return 1

        # transformation #3
        # fixing null values, incorrect main_category
        if table_name=='meta': # products file
            df = transform_meta(df, category_transformation)

        # replacing the table if it's the first chunk & reset param set 'true'
        if reset and i==1:
            if reset.lower()=='true':
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

        t_end = time()
        print(f'... chunk {i:02d} appended, {df.shape[0]} record(s), took {(t_end - t_start):.3f} second(s)')
        t_start = time()

    print(f'Finished ingesting {jsonl_name} into the PostgreSQL database! Total time {(t_end - t_start0):.3f} second(s)\n+++\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest jsonl data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--source', required=True, help='source jsonl file to load')
    parser.add_argument('--reset', required=False, help='True to reset table before loading')

    args = parser.parse_args()

    res = main(args)
    exit(res)