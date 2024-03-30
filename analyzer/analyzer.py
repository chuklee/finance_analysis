import pandas as pd
import numpy as np
import sklearn
import os
import timescaledb_model as tsdb
from datetime import datetime, timezone
import timeit

# db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def store_file(name, website, chunk_size):
    if website.lower() == "boursorama":
        file_path = f"../docker/data/boursorama/{name.split()[1].split('-')[0]}/{name}"
        try:
            df_stocks = pd.read_pickle(file_path)
        except FileNotFoundError:
            print(f"File {file_path} not found.")
            return
        
        if df_stocks.empty:
            print(f"DataFrame loaded from {file_path} is empty.")
            return
        
        market_id = name.split()[0]
        if( market_id == "peapme") :
            df_stocks['pea'] = True
            df_stocks['market_id'] = 0
        else :
            # PEA column is not present so false by default
            df_stocks['pea'] = False
            # Handle different market IDs
            if( market_id == "amsterdam") :
                df_stocks['market_id']  = 6
            elif(market_id == "compA") :
                df_stocks['market_id'] = 7
            elif( market_id == "compB") :
                df_stocks['market_id'] = 8
            else :
                df_stocks['market_id'] = 555
                print("Market ID not found")

        
        df_stocks = (
            df_stocks
            .loc[(df_stocks['last'] != 0) & (df_stocks['volume'] != 0)]
            .rename(columns={'last': 'value'})
            .drop(columns=['symbol'])
        )

        timestamp_str = name.split()[1] + " " + name.split()[2].replace("", ":").replace(".bz2", "")
        try:
            timestamp_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            print(f"Invalid timestamp format: {timestamp_str}")
            return
        
        df_stocks['date'] = pd.Timestamp(timestamp_dt)
        df_stocks['cid'] = 0  # Replace with the actual company ID
        db.df_write(df_stocks, "stocks", chunksize=chunk_size,  commit=True)
       
        # debug
        # print(df_stocks) 

   # to be finished
   

def process_debug_mode(dir, year, n, nb_files) :
    # Line to be removed !!! used to clean the table before processing
    db.clean_stocks_table(commit=True)
    try:
        print("Starting")
        print(datetime.now(timezone.utc))
        # Used to log and debug, to be removed
        """ 
        debug mode
        log_filename = "logs_.txt" + year 
        with open(log_filename, "a", encoding="utf-8") as file_logs:
            files = os.listdir(dir + year)
            for file in files:
                file_logs.write("Processing file: " + file + "\n")
                store_file(file, "boursorama", n) 
                  """
        files = os.listdir(dir + year)
        # pour l'insant on ne fait pas "file_done" pour éviter de perdre du temps ...
        #df_files_done = pd.DataFrame(files, columns=['name'])
        for file in files[:nb_files]:
            store_file(file, "boursorama", n) 
            #df_files_done.append({"name": file} , ignore_index=True, inplace=True)
        # db.df_write(df_files_done, "file_done", chunksize=n, commit=True)
        
        print("Ending")
        print(datetime.now(timezone.utc))
    except Exception as e:
        print("Exception occurred:", e)
        print(datetime.now(timezone.utc))
        exit(1) 


def init_tables():
    # Create the companies and daystocks table because apparently they are not created ?...
    df_companies = pd.DataFrame({
                    'name': [''],
                    'mid': [0],
                    'pea': [False],
                    'symbol': [''],
                })

    db.df_write(df_companies, "companies", index=False,  commit=True)

    """ df_daystocks = pd.DataFrame({
        'cid': [0],
        'date': [pd.Timestamp.now()], # to be changed
        'open': [0],
        'close': [0],
        'high': [0],
        'low': [0],
        'volume': [0],
    })
    db.df_write(df_daystocks, "daystocks", index=False,  commit=True) """


def clean_and_store():
    db.clean_stocks_table(commit=True)
    store_file("compB 2021-09-27 103201.317815.bz2", "boursorama", 10000000)

if __name__ == '__main__':
    print("Starting the process")
    dir = "../docker/data/boursorama/"
    
    # Test sur 1 fichier compB 2021-09-27 103201.317815.bz2
    #clean_and_store()

    # Test sur nombre de fichier
    process_debug_mode(dir , "2019", 10000000, nb_files=10)

    # Create company and day_stocks tables with 1 row of null values
    init_tables()

    db.create_companies_table(commit=True)
    db.create_daystocks_table(commit=True)
    '''
    Uncomment the following lines to process the data
    process_debug_mode("dir , 2020", 10000000)
    process_debug_mode("dir , 2021", 10000000)
    process_debug_mode("dir , 2022", 10000000)
    process_debug_mode("dir , 2023", 10000000)
    '''
    print("Ending the process")
    