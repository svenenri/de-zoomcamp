#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

import pyarrow.parquet as pq

def ingest_from_csv(engine, csv_name, table_name):
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f seconds' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

def ingest_from_parquet(engine, parquet_name, table_name):
    pq_data = pq.read_table(parquet_name)
    pq_data = pq_data.to_pandas()
    t_start = time()
    pq_data.to_sql(name=table_name, con=engine, if_exists='append')
    t_end = time()
    print(f"Total ingestion time: {t_end - t_start} seconds")

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.parquet.gz'):
        parquet_name = 'output.parquet.gz'
        os.system(f"wget {url} -O {parquet_name}")
        ingest_from_parquet(engine=engine, parquet_name=parquet_name, table_name=table_name)
    elif url.endswith('.parquet'):
        parquet_name = 'output.parquet'
        os.system(f"wget {url} -O {parquet_name}")
        ingest_from_parquet(engine=engine, parquet_name=parquet_name, table_name=table_name)
    elif url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
        os.system(f"wget {url} -O {csv_name}")
        ingest_from_csv(engine=engine, csv_name=csv_name, table_name=table_name)
    else:
        csv_name = 'output.csv'
        os.system(f"wget {url} -O {csv_name}")
        ingest_from_csv(engine=engine, csv_name=csv_name, table_name=table_name)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)
