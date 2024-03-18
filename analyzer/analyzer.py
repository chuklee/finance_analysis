import pandas as pd
import numpy as np
import sklearn
import os
import timescaledb_model as tsdb
from datetime import datetime, timezone

#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def store_file(name, website):
    if website.lower() == "boursorama":
        """ try:
            df = pd.read_pickle("../docker/data/boursorama/" + name)  # is this dir ok for you ?
        except:
         """   
        # name = amsterdam 2019-01-01 090502.607291.bz2
        # compB 2019-12-31 171201.644550.bz2
        # amsterdam and compB are the market's name
        market_name = name.split()[0]
        year = name.split()[1].split("-")[0]
        # Split the filename to extract the date and time components
        parts = name.split()
        date_str = parts[1] 
        time_str = parts[2].replace("", ":")  # Extract time component and replace Unicode character with colon
        time_str = time_str.replace(".bz2", "")  # Remove the .bz2 extension from the time component

        # Combine date and time components
        timestamp_str = f"{date_str} {time_str}"

        # Parse the timestamp string into a datetime object
        timestamp_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        df_stocks = pd.read_pickle("../docker/data/boursorama/" + year + "/" + name)
        print(df_stocks)

        df_stocks.loc[:, 'date'] = pd.Timestamp(timestamp_dt)
        # Todo : remove name and replace by cid
        df_stocks.loc[:, 'cid'] = 0
        df_stocks.drop(columns = ['symbol'], inplace = True)
        df_stocks = df_stocks.rename(columns={'last': 'value'})
        # print(df_stocks)
        #db.df_write(df_stocks, "stocks", commit=True)
        

        '''df_companies=''' # to be finished : look into database it will be faster ig
        #df_companies = df_companies.drop_duplicates(subset='name', keep='first')  not used for the moment
        #print(df_companies)
        #db.df_write(df_companies, "companies", commit=True)

    # to be finished
   
if __name__ == '__main__':
    try:
        store_file("amsterdam 2019-01-01 090502.607291.bz2", "boursorama")

        files = os.listdir("../docker/data/boursorama/2019")
        for file in files[:50]:
            print(file)
        print("Done")
    except:
        print("Directory not found")
        exit(1)


     #2021 2022 2023 Y4A DU PEA PME
     # de ce que j'ai vu il existe seulement compA compB amsterdam et peapme ... pme que pour 2021 2022 2023