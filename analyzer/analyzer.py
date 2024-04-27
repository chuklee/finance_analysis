import pandas as pd
import numpy as np
import os
import timescaledb_model as tsdb


# New imports
from datetime import datetime, timezone
import concurrent.futures
import multiprocessing

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker


def clean_value(value):
    # Remove any non-numeric characters and convert to float
    cleaned_value = ''.join(filter(str.isdigit, str(value)))
    return float(cleaned_value) if cleaned_value else np.nan


def store_file(name, website):
    if website.lower() == "boursorama":
        #file_path = f"../docker/data/boursorama/{name.split()[1].split('-')[0]}/{name}"
        file_path = f"/home/bourse/data/boursorama/{name.split()[1].split('-')[0]}/{name}" # inside docker
        
        #file_path = f"data/boursorama/{name.split()[1].split('-')[0]}/{name}"
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
        db.df_write(df_stocks, "stocks", chunksize=100000, index=True,  commit=True)

   # to be finished

# A wrapper function for store_file to unpack the arguments
def store_file_wrapper(args):
    return store_file(*args)

def fill_stocks_for_year(dir, year, max_workers, nb_files=3738) :
    try:
        print("Starting process for directory year " + year)
        begin = datetime.now(timezone.utc)
        files = os.listdir(os.path.join(dir, year))
        # Prepare arguments for each file to be processed
        tasks = [(file, "boursorama") for file in files[:nb_files]]
        #tasks = [(file, "boursorama") for file in files]
        
        # Use ProcessPoolExecutor to process files in parallel
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Map store_file function to the files
            results = list(executor.map(store_file_wrapper, tasks))
        
        # Record which files have been processed
        list_done = [task[0] for task in tasks]
        df_list_done = pd.DataFrame({'name': list_done})
        db.df_write(df_list_done, "file_done", index=False, chunksize=100000, commit=True)
        
        print("Ending process for directory year " + year)
        end = datetime.now(timezone.utc)
        print("Total time for processing year " + year + " : ", end - begin)
    except Exception as e:
        print("Exception occurred:", e)
        print(datetime.now(timezone.utc))
        exit(1) 


# ex 1 file : store_file("compB 2021-09-27 103201.317815.bz2", "boursorama", 10000000)


def resample_group(df):
    return df.resample('D').agg({
        'value': [('open', 'first'), ('close', 'last'), ('high', 'max'), ('low', 'min')],
        'volume': 'max'
    })

def fill_daystocks(df_stocks_generator):
    #print('Daystocks table processing...')

    # Convert generator to a single DataFrame
    df_stocks = pd.concat(df_stocks_generator, ignore_index=True)
    """ 
    Uncomment the following lines if the columns are not already in the correct format
    df_stocks['date'] = pd.to_datetime(df_stocks['date'])
    df_stocks['value'] = pd.to_numeric(df_stocks['value'], errors='coerce')
    df_stocks['volume'] = pd.to_numeric(df_stocks['volume'], errors='coerce')
    """
    df_stocks = df_stocks.set_index('date')
    result = df_stocks.groupby('cid').apply(resample_group, include_groups=False).dropna()
    # Reset index to flatten the DataFrame after groupby
    result = result.reset_index()
    result.columns = ['cid', 'date', 'open', 'close', 'high', 'low', 'volume']
    db.df_write(result, "daystocks", chunksize=10000, index=False, commit=True)

if __name__ == '__main__':
    print("Starting the process")
    #dir = "../docker/data/boursorama/"
    dir = "/home/bourse/data/boursorama/" # inside docker
    

    max_workers = multiprocessing.cpu_count()
    print("Number of workers: ", max_workers)

    begin_whole_process = datetime.now(timezone.utc)
    
    # TEMPORALY alter table stocks
    db.modify_stocks_table(commit=True)

    # 1 Read all files for a specific year
    # 2 Fill stocks table
    # 3 Fill file_done table
    fill_stocks_for_year(dir , "2020", max_workers, nb_files=100) #10 %
    #fill_stocks_for_year(dir , "2020",max_workers , nb_files=100)
    #fill_stocks_for_year(dir , "2020", max_workers)
    """
    fill_stocks_for_year(dir , "2019", max_workers)
    fill_stocks_for_year(dir , "2020", max_workers)
    fill_stocks_for_year(dir , "2021", max_workers)
    fill_stocks_for_year(dir , "2022", max_workers)
    fill_stocks_for_year(dir , "2023", max_workers) 
    """
    
    begin__SQL_time = datetime.now(timezone.utc)
    # Fill companies table
    db.create_companies_table(commit=True)

    # Restore table stocks that has been altered and update the cid column
    db.restore_table(commit=True)
    end_SQL_time = datetime.now(timezone.utc)
    print("Total time for creating SQL tables : ", end_SQL_time - begin__SQL_time)

    df_stocks = db.get_stocks()
    fill_daystocks(df_stocks)   

    end_whole_process = datetime.now(timezone.utc)
    print("Ending the process\nTotal time for the whole process : ", end_whole_process - begin_whole_process)
    