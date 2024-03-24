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
            # Still need to handle the market ID and find it using find company ID
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
        df_companies = pd.DataFrame({
                        'cid': [0],  # Replace with the actual company ID
                        'name': [''],
                        'mid': [0],
                        'pea': [False],
                        'symbol': [''],
                        'isin': ['']
                    })

        # Set the index of the DataFrame to 0
        df_companies.index = [0]
        db.df_write(df_companies, "companies", chunksize=chunk_size,  commit=True)
        # debug
        print(df_stocks) 

   # to be finished
   

def process_debug_mode(dir, year, n) :
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
        for file in files:
            store_file(file, "boursorama", n) 
        
            # df_files_done = pd.DataFrame(files, columns=["name"])
            # db.df_write(df_files_done, "file_done", chunksize=n, commit=True)
        print("Ending")
        print(datetime.now(timezone.utc))
    except Exception as e:
        print("Exception occurred:", e)
        print(datetime.now(timezone.utc))
        exit(1) 

'''
Ideal version of the function
def process(dir, n) :
    try:
        files = os.listdir(dir)
        for file in files:
            store_file(file, "boursorama",n)
    except Exception as e:
        print("Exception occurred:", e)
        print(datetime.now(timezone.utc))
        exit(1) 

'''



if __name__ == '__main__':
    print("Starting the process")
    dir = "../docker/data/boursorama/"
    #store_file("compB 2021-09-27 103201.317815.bz2", "boursorama", 10000000)
    #process_debug_mode(dir , "2019", 10000000)
    '''
    Uncomment the following lines to process the data
    process_debug_mode("dir , 2020", 10000000)
    process_debug_mode("dir , 2021", 10000000)
    process_debug_mode("dir , 2022", 10000000)
    process_debug_mode("dir , 2023", 10000000)
    '''
    db.create_companies_table(commit=True)
    print("Ending the process")
    