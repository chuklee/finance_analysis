import pandas as pd
import numpy as np
import os
import timescaledb_model as tsdb


# New imports
from datetime import datetime, timezone
import concurrent.futures

# db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def clean_value(value):
    # Remove any non-numeric characters and convert to float
    cleaned_value = ''.join(filter(str.isdigit, str(value)))
    return float(cleaned_value) if cleaned_value else np.nan


def store_file(name, website, chunk_size):
    if website.lower() == "boursorama":
        file_path = f"../docker/data/boursorama/{name.split()[1].split('-')[0]}/{name}"
        try:
            df_stocks = pd.read_pickle(file_path)
            #print(df_stocks)
        except FileNotFoundError:
            print(f"File {file_path} not found.")
            return
        
        if df_stocks.empty:
            print(f"DataFrame loaded from {file_path} is empty.")
            return
        
        mid = name.split()[0]
        if( mid == "peapme") :
            df_stocks['pea'] = True
            df_stocks['mid'] = 0
        else :
            # PEA column is not present so false by default
            df_stocks['pea'] = False
            # Handle different market IDs
            if( mid == "amsterdam") :
                df_stocks['mid']  = 6
            elif(mid == "compA") :
                df_stocks['mid'] = 7
            elif( mid == "compB") :
                df_stocks['mid'] = 8
            else :
                df_stocks['mid'] = 555
                print("Market ID not found")

        
        # Clean 'value' column
        df_stocks['last'] = df_stocks['last'].apply(clean_value)

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
        df_stocks['cid'] = 0 
        db.df_write(df_stocks, "stocks", chunksize=1000, index=True,  commit=True)
       

   # to be finished
        
        
def store_file_wrapper(args):
    # A wrapper function for store_file to unpack the arguments
    return store_file(*args)

def process_debug_mode(dir, year, n, nb_files) :
    try:
        print("Starting process for directory year " + year)
        begin = datetime.now(timezone.utc)
        files = os.listdir(os.path.join(dir, year))
        # Prepare arguments for each file to be processed
        tasks = [(file, "boursorama", n) for file in files[:nb_files]]
        
        # Use ProcessPoolExecutor to process files in parallel
        with concurrent.futures.ProcessPoolExecutor(max_workers=20) as executor:
            # Map store_file function to the files
            results = list(executor.map(store_file_wrapper, tasks))
        
        # Assuming you still want to record which files have been processed
        list_done = [task[0] for task in tasks]
        df_list_done = pd.DataFrame({'name': list_done})
        db.df_write(df_list_done, "file_done", index=False, chunksize=n, commit=True)
        
        print("Ending process for directory year " + year)
        end = datetime.now(timezone.utc)
        print("Total time for processing year " + year + " : ", end - begin)
    except Exception as e:
        print("Exception occurred:", e)
        print(datetime.now(timezone.utc))
        exit(1) 


# ex 1 file : store_file("compB 2021-09-27 103201.317815.bz2", "boursorama", 10000000)

if __name__ == '__main__':
    print("Starting the process")
    dir = "../docker/data/boursorama/"
    
    #db.modify_stocks_table(commit=True)
    # Test sur nombre de fichier
    #process_debug_mode(dir , "2020", 1000, nb_files=3738)
    process_debug_mode(dir , "2020", 1000, nb_files=1500)
    
    begin_time = datetime.now(timezone.utc)
    db.create_companies_table(commit=True)
    #db.create_daystocks_table(commit=True) does not work yet

    db.restore_table(commit=True)
    end_time = datetime.now(timezone.utc)
    print("Total time for creating SQL tables : ", end_time - begin_time)
    '''
    print('Begin total process (create tables companies and daystocks) at :', datetime.now(timezone.utc))
    print('Total process ended at :', datetime.now(timezone.utc))
    
    Uncomment the following lines to process the data

    todo : process upside down and sort files by date DESC
    process_debug_mode("dir , 2023", 10000000)
    process_debug_mode("dir , 2022", 10000000)
    process_debug_mode("dir , 2021", 10000000)
    process_debug_mode("dir , 2020", 10000000)
    process_debug_mode("dir , 2019", 10000000)
    '''
    print("Ending the process")
    