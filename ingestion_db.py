import pandas as pd
import os
from sqlalchemy import create_engine, text
import logging
import time


logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine('sqlite:///inventory.db')
def table_exists(table_name, engine):
    '''Check if a table exists in SQLite'''
    query = text("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=:table_name
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {'table_name': table_name}).fetchone()
        return result is not None

def truncate_table(table_name, engine):
    '''Deletes all rows from the table (SQLite compatible)'''
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM `{table_name}`;"))


def ingest_db(file_path, table_name, engine):
    '''Ingests CSV into SQLite in chunks after deleting previous data'''
    if not table_exists(table_name, engine):
        print(f"Table '{table_name}' does not exist, creating it...")
        logging.info(f"Table '{table_name}' does not exist, creating it...")
        print(f" Ingesting into Table '{table_name}' ")
        logging.info(f"Ingesting into Table '{table_name}'")
        
        for chunk in pd.read_csv(file_path, chunksize=10000):
            chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
        
        return
    logging.info(f'trauncating table:"{table_name}"')
    print(f'trauncating table:"{table_name}"')
    truncate_table(table_name, engine)
    
    for chunk in pd.read_csv(file_path, chunksize=10000):
        chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
   


def load_raw_data():
    '''Loads all CSVs from "data" folder and ingests into DB'''
    start = time.time()
    print('----------Ingestion Started----------')
    logging.info('----------Ingestion Started----------')
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            file_path = os.path.join('data', file)
            table_name = file[:-4]
            logging.info(f'Ingesting {file} into DB table "{table_name}"')
            print(f"Ingesting {file} into DB")
            ingest_db(file_path, table_name, engine)
            print(f'Ingested {file} into DB table "{table_name}"')
            logging.info(f'Ingested {file} into DB table "{table_name}"')
    end = time.time()
    total_time = (end - start)/60
    
    logging.info('----------Ingestion Complete----------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')
    print('----------Ingestion Complete----------')
    print(f'Total Time Taken: {total_time:.2f} minutes')


if __name__ == '__main__':
    load_raw_data()
