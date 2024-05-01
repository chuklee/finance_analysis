import pandas as pd
import numpy as np
import os
import timescaledb_model as tsdb


# New imports
from datetime import datetime, timezone
import concurrent.futures
import multiprocessing

#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker


def clean_value(value):
    # Remove any non-numeric characters except decimal point
    cleaned_value = ''.join(c for c in str(value) if c.isdigit() or c == '.')
    # Convert to float
    return float(cleaned_value) if cleaned_value else np.nan


def store_file(name, website):
    if website.lower() == "boursorama":
        file_path = f"../docker/data/boursorama/{name.split()[1].split('-')[0]}/{name}" # outside docker
        #file_path = f"/home/bourse/data/{name.split()[1].split('-')[0]}/{name}" # inside docker
        try:
            df_stocks = pd.read_pickle(file_path)
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

        
        # Clean 'value' column
        df_stocks['last'] = df_stocks['last'].apply(clean_value)

        df_stocks = (
            df_stocks
            .loc[(df_stocks['last'] != 0) & (df_stocks['volume'] != 0) & (df_stocks['value'] < 2147483647) ]
            .rename(columns={'last': 'value'})
            .drop(columns=['symbol'])
        )
        #timestamp_str = name.split()[1] + " " + name.split()[2].replace("", ":").replace(".bz2", "") # to be removed
        timestamp_str = name.split()[1] + " " + name.split()[2].replace("_", ":").replace(".bz2", "")
        try:
            timestamp_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            print(f"Invalid timestamp format: {timestamp_str}")
            return
        
        df_stocks['date'] = pd.Timestamp(timestamp_dt)
        df_stocks['cid'] = 0 
        db.df_write(df_stocks, "stocks", chunksize=100000, index=True,  commit=True)

# A wrapper function for store_file to unpack the arguments
def store_file_wrapper(args):
    return store_file(*args)


#def fill_stocks_for_year(dir, year, max_workers, nb_files=3738) : # to be removed
def fill_stocks_for_year(dir, year, max_workers):
    try:
        print("Starting process for directory year " + year) # to be removed
        begin = datetime.now(timezone.utc) # to be removed
        files = os.listdir(os.path.join(dir, year))
        # Get the total number of files
        num_files = len(files)

        # Calculate starting and ending indices (rounded down) # to be removed
        #start_index = num_files // 2 # to be removed 
        #end_index = num_files * 3 // 4 # to be removed

        # Create tasks for files from the starting index (inclusive) to the ending index (exclusive) # to be removed
        #tasks = [(file, "boursorama") for file in files[start_index:end_index]] # to be removed
        tasks = [(file, "boursorama") for file in files] 
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(store_file_wrapper, tasks))
        list_done = [task[0] for task in tasks]
        df_list_done = pd.DataFrame({'name': list_done})
        db.df_write(df_list_done, "file_done", index=False, chunksize=100000, commit=True)
        print("Ending process for directory year " + year) # to be removed
        end = datetime.now(timezone.utc) # to be removed
        print("Total time for processing year " + year + " : ", end - begin) # to be removed
    except Exception as e:
        print("Exception occurred:", e)
        exit(1)

def resample_group(df):
    return df.resample('D').agg({
        'value': [('open', 'first'), ('close', 'last'), ('high', 'max'), ('low', 'min')],
        'volume': 'max'
    })

def fill_daystocks(df_stocks_generator):
    # Convert generator to a single DataFrame
    df_stocks = pd.concat(df_stocks_generator, ignore_index=True)
    df_stocks = df_stocks.set_index('date')
    result = df_stocks.groupby('cid').apply(resample_group, include_groups=False).dropna()
    # Reset index to flatten the DataFrame after groupby
    result = result.reset_index()
    result.columns = ['cid', 'date', 'open', 'close', 'high', 'low', 'volume']
    db.df_write(result, "daystocks", chunksize=100000, index=False, commit=True)


def process_chunk(chunk_info):
    offset, chunksize = chunk_info
    print(f'Processing chunk at offset {offset} at {datetime.now(timezone.utc)}')
    chunk = db.get_stocks(chunksize=chunksize, offset=offset)
    fill_daystocks(chunk)
    print(f'Chunk at offset {offset} done at {datetime.now(timezone.utc)}')

if __name__ == '__main__':
    print(f"Starting the process at {datetime.now(timezone.utc)}") # to be removed
    dir = "../docker/data/boursorama/" # outside docker
    #dir = "/home/bourse/data/" # inside docker
    

    max_workers = multiprocessing.cpu_count()
    print("Number of workers: ", max_workers)

    begin_whole_process = datetime.now(timezone.utc)
    # Alter table daystocks for volumes, setting them to bigint
    db.modify_daystocks_table(commit=True)
    # TEMPORALY alter table stocks
    db.modify_stocks_table(commit=True)
    
    print(f'db.modify_stocks_table done at {datetime.now(timezone.utc)}') # to be removed
    fill_stocks_for_year(dir , "2019", max_workers)

    print(f'fill_stocks_for_year 2019 done at {datetime.now(timezone.utc)}') # to be removed
    fill_stocks_for_year(dir , "2020", max_workers)
    print(f'fill_stocks_for_year 2020 done at {datetime.now(timezone.utc)}') # to be removed
    
    fill_stocks_for_year(dir , "2021", max_workers)
    print(f'fill_stocks_for_year 2021 done at {datetime.now(timezone.utc)}') # to be removed

    fill_stocks_for_year(dir , "2022", max_workers)
    print(f'fill_stocks_for_year 2022 done at {datetime.now(timezone.utc)}') # to be removed

    fill_stocks_for_year(dir , "2023", max_workers) 
    print(f'fill_stocks_for_year 2023 done at {datetime.now(timezone.utc)}') # to be removed

   
    begin__SQL_time = datetime.now(timezone.utc) 
    db.create_companies_table(commit=True)
    print(f'db.create_companies_table done at {datetime.now(timezone.utc)}') # to be removed
    db.restore_table(commit=True)
    print(f'db.restore_table (stocks) done at {datetime.now(timezone.utc)}') # to be removed
   
    chunksize=2000000  
    offset = 0
    i = 0
    # Nombre de rows stocks total : 157713346
    #stocks_len = db.count_stocks()
    stocks_len = 157713346
    print("Nombre de rows stocks total : ", stocks_len)
    
    # Create a list of chunk offsets and sizes
    chunk_infos = [(offset, chunksize) for offset in range(0, stocks_len, chunksize)]
    
    # Create a multiprocessing pool with the desired number of processes
    pool = multiprocessing.Pool(processes=max_workers)
    
    # Process the chunks in parallel using the pool
    pool.map(process_chunk, chunk_infos)
    
    # Wait for all processes to finish
    pool.close()
    pool.join()

    print(f'fill_daystocks done at {datetime.now(timezone.utc)}') # to be removed
    end_SQL_time = datetime.now(timezone.utc) # to be removed
    #print("Total time for creating SQL tables (companies, update stocks and daystocks) : ", end_SQL_time - begin__SQL_time) # to be removed

    end_whole_process = datetime.now(timezone.utc)
    print("Ending the process\nTotal time for the whole process : ", end_whole_process - begin_whole_process)
    print('Done')