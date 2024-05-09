import pandas as pd
import numpy as np
import os
import timescaledb_model as tsdb
# New imports
from datetime import datetime, timezone
import multiprocessing as mp  # Sheesh

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker



def clean_value(value):
    cleaned_value = ''.join(c for c in str(value) if c.isdigit() or c == '.')
    return float(cleaned_value) if cleaned_value else np.nan

def store_file(name, website):
    if website.lower() == "boursorama":
        #file_path = f"../docker/data/boursorama/{name.split()[1].split('-')[0]}/{name}" # outside docker
        file_path = f"/home/bourse/data/{name.split()[1].split('-')[0]}/{name}" # inside docker
        df_stocks = pd.read_pickle(file_path)
        if df_stocks.empty:
            return pd.DataFrame()
        
        mid_dict = {"peapme": 999, "amsterdam": 6, "compA": 7, "compB": 8}
        mid = mid_dict.get(name.split()[0], 1)
        peapme = mid == 999
        df_stocks['last'] = df_stocks['last'].apply(clean_value)
        df_stocks = (
            df_stocks
            .loc[(df_stocks['last'] != 0) & (df_stocks['volume'] != 0) & (df_stocks['last'] < 2147483647)]
            .rename(columns={'last': 'value'})
        )
        #timestamp_str = name.split()[1] + " " + name.split()[2].replace("", ":").replace(".bz2", "") # to be removed
        timestamp_str = name.split()[1] + " " + name.split()[2].replace("_", ":").replace(".bz2", "")
        df_stocks['date'] = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        
       
        
        # Vectorized operation to find or insert companies
        symbols = df_stocks['symbol'].to_numpy()
        names = df_stocks['name'].to_numpy()
        cids = np.array([db.search_company_id_by_symbol(symbol) or db.insert_companies(name, peapme, mid, symbol, commit=True)
                        for symbol, name in zip(symbols, names)])
        df_stocks['cid'] = cids.astype(int)
        df_stocks = df_stocks[['date', 'cid', 'value', 'volume']]
        db.df_write_copy(df_stocks, "stocks",  commit=True)
        return df_stocks

def process_file(file_queue, df_queue):
    while True:
        try:
            file = file_queue.get(timeout=1)
            # uncomment the following line to see the progress of the process
            # print(f"Processing file: {file} at {datetime.now(timezone.utc)}") # to be removed
            df_stocks = store_file(file, "boursorama")
            df_queue.put(df_stocks)
        except mp.queues.Empty:
            break

def fill_stocks_for_year(dir, year, nb_files=10):
    try:
        # Change here to get all files
        files = os.listdir(os.path.join(dir, year))[:nb_files]
        
        file_queue = mp.Queue()
        df_queue = mp.Queue()

        for file in files:
            file_queue.put(file)

        processes = []
        for _ in range(mp.cpu_count()):
            p = mp.Process(target=process_file, args=(file_queue, df_queue))
            p.start()
            processes.append(p)

        list_df_stocks_done = []
        while True:
            try:
                df = df_queue.get(timeout=1)
                list_df_stocks_done.append(df)
            except mp.queues.Empty:
                if all(not p.is_alive() for p in processes):
                    break

        for p in processes:
            p.join()

        df_files_done = pd.DataFrame({'name': files})
        db.df_write_copy(df_files_done, "file_done", commit=True)
        
        concatened_df = pd.concat(list_df_stocks_done, ignore_index=True)
        fill_daystocks(concatened_df)

    except Exception as e:
        print("Exception occurred:", e)
        exit(1)


def resample_group(df):
    return df.resample('D').agg({
        'value': [('open', 'first'), ('close', 'last'), ('high', 'max'), ('low', 'min')],
        #'volume': 'max'
        'volume': 'sum'
    })



def fill_daystocks(df):
    df = df.set_index('date')
    result = df.groupby('cid').apply(resample_group, include_groups=False).dropna()
    # Reset index to flatten the DataFrame after groupby
    result = result.reset_index()
    result.columns = ['cid', 'date', 'open', 'close', 'high', 'low', 'volume']
    result = result[['cid', 'date', 'open', 'close', 'high', 'low', 'volume']]
    db.df_write_copy(result, "daystocks", commit=True)



if __name__ == '__main__':
    dir = "/home/bourse/data/"
    #dir = "../docker/data/boursorama/"
    print("Start")
    begin_whole_process = datetime.now(timezone.utc)
    db.set_volume_bigint()
    #store_file("amsterdam 2019-01-01 090502.607291.bz2" , "boursorama")
    store_file("amsterdam 2019-01-01 09_05_02.607291.bz2" , "boursorama")
    fill_stocks_for_year(dir, "2019", nb_files=10)
    """ fill_stocks_for_year(dir, "2020", nb_files=10)
    fill_stocks_for_year(dir, "2021", nb_files=10)
    fill_stocks_for_year(dir, "2022", nb_files=10)
    fill_stocks_for_year(dir, "2023", nb_files=10) """
    

    end_whole_process = datetime.now(timezone.utc)
    print(f"Whole process done in {end_whole_process - begin_whole_process}")
    print('Done')