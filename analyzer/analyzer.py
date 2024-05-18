import pandas as pd
import numpy as np
import os
import timescaledb_model as tsdb


# New imports
from datetime import datetime, timezone

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

existing_companies = set()
current_month = 1

def clean_value(value):
    cleaned_value = ''.join(c for c in str(value) if c.isdigit() or c == '.')
    return float(cleaned_value) if cleaned_value else np.nan


def store_file(name, website, df_companies):
    if website.lower() == "boursorama":
        name = name.replace("_", ":")

        #file_path = f"../docker/data/boursorama/{name.split()[1].split('-')[0]}/{name}" # outside docker
        file_path = f"/home/bourse/data/{name.split()[1].split('-')[0]}/{name}" # inside docker

        df_stocks = pd.read_pickle(file_path)
        if df_stocks.empty:
            return
        
        
        #name = name.replace("", ":").replace(".bz2", "")
        #print('name = ' , name , flush=True)

        # Clean 'value' column
        df_stocks['last'] = df_stocks['last'].apply(clean_value)
        df_stocks = (
            df_stocks
            .drop_duplicates(subset=['last'])
            .loc[(df_stocks['last'] != 0) & (df_stocks['volume'] != 0) & (df_stocks['last'] < 2147483647) ]
            .rename(columns={'last': 'value'})
        )

        df_stocks.reset_index(drop=True, inplace=True)
        # Ensure 'df_companies' contains both 'symbol' and 'cid' columns
        if {'symbol', 'id'}.issubset(df_companies.columns):
            # Create a mapping of 'symbol' to 'cid'
            symbol_to_cid_map = df_companies.set_index('symbol')['id']
            # Add 'cid' column to df_stocks
            df_stocks['cid'] = df_stocks['symbol'].map(symbol_to_cid_map)
        else:
            raise KeyError("DataFrame df_companies must contain both 'symbol' and 'cid' columns.")
        
        name = name.replace("_", ":").replace(".bz2", "")

        timestamp_str = name.split()[1] + " " + name.split()[2] # to be removed
        date =  datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        df_stocks['date'] = date
        df_stocks = df_stocks[['date', 'cid', 'value', 'volume']]
        # remove where cid is null
        df_stocks = df_stocks.dropna(subset=['cid'])
        df_stocks['cid'] = df_stocks['cid'].astype(int)
        db.df_write_copy(df_stocks, "stocks", commit=True)
        return df_stocks, name, date.month
         



def fill_stocks_for_year(dir, year, df_companies, nb_files=10):
    try:
        global current_month
        files = os.listdir(os.path.join(dir, year))
        files.sort()
        files = files[:100]
        list_df_stocks_done = []
        for file in files:
          df , name, month = store_file(file, "boursorama", df_companies)
          print(f"Processing file : {name}" , flush=True) # to be removed
          list_df_stocks_done.append(df)
          db.df_write_copy(pd.DataFrame({'name': [name]}), "file_done", commit=True)
        # todo mettre le if ici 
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




# For each year, fill the companies table
   
def fill_companies_table(dir, year):
    files = os.listdir(os.path.join(dir, year))
    files.sort()
    # Dictionary to store the earliest file of each day
    daily_files = {}

    for file in files:
        # Extract date and time from filename
        parts = file.split()
        date_str = parts[1]
        time_str = parts[2].replace("", ":").replace(".bz2", "")
        
        # Parse timestamp
        timestamp = datetime.strptime(date_str + " " + time_str, "%Y-%m-%d %H:%M:%S.%f")
        
        # Group files by day and find the earliest file
        date_key = date_str
        if date_key not in daily_files:
            daily_files[date_key] = (timestamp, file)
        else:
            if timestamp < daily_files[date_key][0]:
                daily_files[date_key] = (timestamp, file)

    selected_files = [daily_files[day][1] for day in daily_files]

    # Set to keep track of existing companies
    global existing_companies
    for file in selected_files:
        df = pd.read_pickle(os.path.join(dir, year, file))
        df.drop_duplicates(subset=['symbol'], inplace=True)
        mid_dict = {"peapme": 999, "amsterdam": 6, "compA": 7, "compB": 8}
        mid = mid_dict.get(file.split()[0], 1)
        peapme = mid == 999
        df['mid'] = mid
        df['pea'] = peapme
        df.drop(columns=['last', 'volume'], inplace=True)

        # Filter out companies already present
        new_companies = df[~df['symbol'].isin(existing_companies)]
        
        # Update the set with the new companies
        existing_companies.update(new_companies['symbol'].tolist())

        if not new_companies.empty:
            db.df_write_copy(new_companies, "companies", commit=True)
    

if __name__ == '__main__':
    print('Start', flush=True)
    dir = "/home/bourse/data/"
    #dir = "../docker/data/boursorama/"
    
    begin_whole_process = datetime.now(timezone.utc)
    begin_companies = datetime.now(timezone.utc)
    fill_companies_table(dir, "2019")
    fill_companies_table(dir, "2020")
    fill_companies_table(dir, "2021")
    fill_companies_table(dir, "2022")
    fill_companies_table(dir, "2023")
    print(f"Companies done at {datetime.now(timezone.utc)} in {datetime.now(timezone.utc) - begin_companies}", flush=True)
    db.set_volume_bigint()
    # Using pandas merge to join stocks with companies on 'symbol', setting 'cid' as index
    df_companies = db.get_companies()
    # Convert df generator object to dataframe
    df_companies = pd.concat(list(df_companies), ignore_index=True)
    fill_stocks_for_year(dir , "2019", df_companies)
    print(f"2019 done at {datetime.now(timezone.utc)}" , flush=True)
    fill_stocks_for_year(dir , "2020", df_companies)
    print(f"2020 done at {datetime.now(timezone.utc)}", flush=True)
    fill_stocks_for_year(dir , "2021", df_companies)
    print(f"2021 done at {datetime.now(timezone.utc)}", flush=True)
    fill_stocks_for_year(dir , "2022", df_companies)
    print(f"2022 done at {datetime.now(timezone.utc)}", flush=True)
    fill_stocks_for_year(dir , "2023", df_companies)
    print(f"2023 done at {datetime.now(timezone.utc)}", flush=True)

    
    print(f'fill_daystocks done at {datetime.now(timezone.utc)}', flush=True) # to be removed
    end_whole_process = datetime.now(timezone.utc)
    print(f"Whole process done in {end_whole_process - begin_whole_process}", flush=True)
    print('Done', flush=True)