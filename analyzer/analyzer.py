import pandas as pd
import numpy as np
import sklearn
import os
import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def store_file(name, website):
   
    if website.lower() == "boursorama":
        print("ouHOOO")
        try:
            df = pd.read_pickle("/docker/data/boursorama/" + name)  # is this dir ok for you ?
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("../docker/data/boursorama/" + year + "/" + name)
        print(df)
        
        df_companies= df[['name', 'symbol']]
        df_companies = df_companies.drop(columns=['symbol'])
        #df_companies = df_companies.drop_duplicates(subset='name', keep='first')  not used for the moment
        print(df_companies)
        db.df_write(df_companies, "companies", commit=True)

        ''' 
        Does not work since the company id is not handled for the moment ...

        df_stocks = df[['last', 'volume', 'name']]

        df_stocks = df_stocks.rename(columns={'last': 'value'})
        for index, row in df_stocks.iterrows(): 
            # Extract company name and symbol from the row
            company_name = row['name']
            
            company_id = db.search_company_id(company_name)
            
            # Set the 'cid' column in the current row to the company ID
            df_stocks.at[index, 'cid'] = company_id
        print(df_stocks)
        '''
    # to be finished
    """ 
    #Print the companies table
    request = db.get_companies()
    for r in request:
        print(r)
     """
if __name__ == '__main__':
    store_file("amsterdam 2019-01-01 090502.607291.bz2", "boursorama")
    print("Done")
