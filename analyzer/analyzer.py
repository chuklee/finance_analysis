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
    cleaned_value = ''.join(c for c in str(value) if c.isdigit() or c == '.')
    return float(cleaned_value) if cleaned_value else np.nan


def store_file(name, website):
    if website.lower() == "boursorama":
        #file_path = f"../docker/data/boursorama/{name.split()[1].split('-')[0]}/{name}" # outside docker
        file_path = f"/home/bourse/data/{name.split()[1].split('-')[0]}/{name}" # inside docker
        df_stocks = pd.read_pickle(file_path)
        
        if df_stocks.empty:
            return
        
        mid = name.split()[0]
        if( mid == "peapme") :
            df_stocks['pea'] = True
            df_stocks['mid'] = 0
        else :
            df_stocks['pea'] = False
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
            .loc[(df_stocks['last'] != 0) & (df_stocks['volume'] != 0) & (df_stocks['last'] < 2147483647) ]
            .rename(columns={'last': 'value'})
        )
        #timestamp_str = name.split()[1] + " " + name.split()[2].replace("", ":").replace(".bz2", "") # to be removed
        timestamp_str = name.split()[1] + " " + name.split()[2].replace("_", ":").replace(".bz2", "")
        df_stocks['date'] = pd.Timestamp(datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f"))
        df_stocks['cid'] = 0 
        df_stocks = df_stocks[['date', 'cid', 'value', 'volume', 'name', 'pea', 'mid', 'symbol']]
        db.df_write_copy(df_stocks, "stocks", chunksize=100000, index=False,  commit=True)
        



def fill_stocks_for_year(dir, year, nb_files=10000) : # to be removed
    try:
        files = os.listdir(os.path.join(dir, year))
        list_files_done = []
        for file in files[:nb_files] :
        #for file in files :
            store_file(file, "boursorama")
            list_files_done.append(file)
        df_files_done = pd.DataFrame({'name': list_files_done})
        db.df_write_copy(df_files_done, "file_done", index=False, chunksize=10000, commit=True)

    except Exception as e:
        print("Exception occurred:", e)
        exit(1)



# Début des fonctions pour remplir la table daystocks
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
    result = result[['cid', 'date', 'open', 'close', 'high', 'low', 'volume']]
    result['volume'] = result['volume'].astype(np.int64)
    db.df_write_copy(result, "daystocks", chunksize=100000, index=False, commit=True)


def process_chunk(chunk_info):
    offset, chunksize = chunk_info
    chunk = db.get_stocks(chunksize=chunksize, offset=offset)
    fill_daystocks(chunk)
# Fin des fonctions pour remplir la table daystocks



if __name__ == '__main__':
    dir = "/home/bourse/data/"
    
    begin_whole_process = datetime.now(timezone.utc)
    db.modify_daystocks_table(commit=True)
    db.modify_stocks_table(commit=True)

    fill_stocks_for_year(dir , "2019", 1000)
    print("2019 done")
    fill_stocks_for_year(dir , "2020", 1000)
    print("2020 done")
    fill_stocks_for_year(dir , "2021" , 1000)
    print("2021 done")
    fill_stocks_for_year(dir , "2022", 1000)
    print("2022 done")
    fill_stocks_for_year(dir , "2023", 1000)
    print("2023 done")

   
    db.create_companies_table(commit=True)
    db.restore_table2(commit=True)
    chunksize=2000000  
    offset = 0
    i = 0
    # Nombre de rows stocks total : 157713346
    stocks_len = db.count_stocks()
    #stocks_len = 157713346
    print("Nombre de rows stocks total : ", stocks_len)
    
    # Create a list of chunk offsets and sizes
    chunk_infos = [(offset, chunksize) for offset in range(0, stocks_len, chunksize)]
    for chunk_info in chunk_infos:
        process_chunk(chunk_info)
        i += 1
        print(f"Chunk {i} done")
    
    print(f'fill_daystocks done at {datetime.now(timezone.utc)}') # to be removed
    end_whole_process = datetime.now(timezone.utc)
    print(f"Whole process done in {end_whole_process - begin_whole_process}")
    print('Done')