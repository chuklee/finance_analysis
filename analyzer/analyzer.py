import pandas as pd
import numpy as np
import os
import timescaledb_model as tsdb
# New imports
from datetime import datetime, timezone

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

current_month = 1


def clean_value(value):
    cleaned_value = ''.join(c for c in str(value) if c.isdigit() or c == '.')
    return float(cleaned_value) if cleaned_value else np.nan

def store_file(name, website):
    if website.lower() == "boursorama":
        #file_path = f"../docker/data/boursorama/{name.split()[1].split('-')[0]}/{name}" # outside docker
        name = name.replace("_", ":")
        file_path = f"/home/bourse/data/{name.split()[1].split('-')[0]}/{name}" # inside docker
        #file_path = f"/home/bourse/data/{name}" 
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
        date =  datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        df_stocks['date'] = date
        
        # Vectorized operation to find or insert companies
        symbols = df_stocks['symbol'].to_numpy()
        names = df_stocks['name'].to_numpy()
        cids = np.array([db.search_company_id_by_symbol(symbol) or db.insert_companies(name, peapme, mid, symbol, commit=True)
                        for symbol, name in zip(symbols, names)])
        df_stocks['cid'] = cids.astype(int)
        df_stocks = df_stocks[['date', 'cid', 'value', 'volume']]
        db.df_write_copy(df_stocks, "stocks",  commit=True)
        #print(f"timestamp_str . day = {date.day}")
        return df_stocks , name, date.month


def fill_stocks_for_year(dir, year, nb_files=10):
    try:
        global current_month
        files = os.listdir(os.path.join(dir, year))
        files.sort()
        #files = files[:110]
        #print(files)
        list_df_stocks_done = []
        for file in files:
          df , name, month = store_file(file, "boursorama")
          #print(f"Processing file : {name}") # to be removed
          list_df_stocks_done.append(df)
          db.df_write_copy(pd.DataFrame({'name': [name]}), "file_done", commit=True)
          if month != current_month :
            concatened_df = pd.concat(list_df_stocks_done, ignore_index=True)
            fill_daystocks(concatened_df)
            list_df_stocks_done = []
            current_month = month

    except Exception as e:
        print("Exception occurred:", e)
        exit(1)


def resample_group(df):
    return df.resample('D').agg({
        'cid' : 'first',
        'value': [('open', 'first'), ('close', 'last'), ('high', 'max'), ('low', 'min')],
        #'volume': 'max'
        'volume': 'sum'
    })



def fill_daystocks(df):
    df = df.set_index('date')
    
    #result = df.groupby('cid').apply(resample_group, include_groups=False).dropna()
    result = df.groupby('cid', group_keys=False).apply(resample_group).dropna()
    # Reset index to flatten the DataFrame after groupby
    result = result.reset_index()
    result.columns = ['date', 'cid', 'open', 'close', 'high', 'low', 'volume']
    # Reorder columns
    result = result[['cid', 'date', 'open', 'close', 'high', 'low', 'volume']]
    result['cid'] = result['cid'].astype(int)
    db.df_write_copy(result, "daystocks", commit=True)



if __name__ == '__main__':
    dir = "/home/bourse/data/"
    #dir = "../docker/data/boursorama/"
    print("Start", flush=True)
    begin_whole_process = datetime.now(timezone.utc)
    db.set_volume_bigint()
    #store_file("amsterdam 2019-01-01 090502.607291.bz2" , "boursorama")
    #store_file("amsterdam 2019-01-01 09_05_02.607291.bz2" , "boursorama")


    begin2019 = datetime.now(timezone.utc)
    fill_stocks_for_year(dir, "2019")
    print(f'2019 took {datetime.now(timezone.utc) - begin2019}', flush=True)
    begin2020 = datetime.now(timezone.utc)
    fill_stocks_for_year(dir, "2020")
    print(f'2020 took {datetime.now(timezone.utc) - begin2020}', flush=True)
    begin2021 = datetime.now(timezone.utc)
    fill_stocks_for_year(dir, "2021")
    print(f'2021 took {datetime.now(timezone.utc) - begin2021}', flush=True)
    begin2022 = datetime.now(timezone.utc)
    fill_stocks_for_year(dir, "2022")
    print(f'2022 took {datetime.now(timezone.utc) - begin2022}', flush=True)
    begin2023 = datetime.now(timezone.utc)
    fill_stocks_for_year(dir, "2023")
    print(f'2023 took {datetime.now(timezone.utc) - begin2023}', flush=True)

    end_whole_process = datetime.now(timezone.utc)
    print(f"Whole process done in {end_whole_process - begin_whole_process}", flush=True)
    print('Done', flush=True)