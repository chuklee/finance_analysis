import timeit
import pandas as pd
import pandas as pd
import numpy as np
import timescaledb_model as tsdb
from datetime import datetime, timezone
from analyzer.analyzer import store_file_old, store_file_new


'''THIS FILE IS A BENCHMARK FILE TO COMPARE THE PERFORMANCE OF TWO METHODS IN analyzer/analyzer.py
    IT IS MEANT TO BE RUN IN THE ROOT DIRECTORY OF THE PROJECT
    AND REMOVED FOR THE FINAL VERSION OF THE PROJECT
'''

#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker
# Define the two methods

def method_chaining(df_stocks):
    db.df_write(tmp_chaining(df_stocks), "stocks", commit=False)


def tmp_chaining(df_stocks):
    return (
        df_stocks
        .loc[(df_stocks['last'] != 0) & (df_stocks['volume'] != 0)]
        .rename(columns={'last': 'value'})
        .drop(columns=['symbol'])
    )

def separate_operations(df_stocks):
    df_stocks = df_stocks[df_stocks['last'] != 0]
    df_stocks = df_stocks[df_stocks['volume'] != 0]
    df_stocks.drop(columns=['symbol'], inplace=True)
    df_stocks = df_stocks.rename(columns={'last': 'value'})
    db.df_write(df_stocks, "stocks", commit=False)
    return df_stocks

# Create a sample DataFrame
""" df_stocks = pd.DataFrame({
    'symbol': ['AAPL', 'GOOGL', 'MSFT'],
    'last': [100, 200, 0],
    'volume': [1000000, 2000000, 0]
}) """
df_stocks_global = pd.read_pickle("../docker/data/boursorama/2019/amsterdam 2019-01-01 090502.607291.bz2")

# Benchmark the methods
method_chaining_time = timeit.timeit(lambda: method_chaining(df_stocks_global.copy()), number=300)
separate_operations_time = timeit.timeit(lambda: separate_operations(df_stocks_global.copy()), number=300)

# Print the results
print("Method Chaining Time:", method_chaining_time)
print("Separate Operations Time:", separate_operations_time)

""" method_old = timeit.timeit(lambda: store_file_old("amsterdam 2019-01-01 090502.607291.bz2", "boursorama"), number=1)
method_new = timeit.timeit(lambda: store_file_new("amsterdam 2019-01-01 090502.607291.bz2", "boursorama"), number=1)

# Print the results
print("method_old Time:", method_old)
print("method_new Time:", method_new) """
# Benchmark the methods


 
   
"""
def process_old(n) :
    try:
        files = os.listdir("../docker/data/boursorama/2021")
        for file in files[:563]:
            store_file_old(file, "boursorama",n)
    except Exception as e:
        print("Exception occurred:", e)
        print(datetime.now(timezone.utc))
        exit(1) 


def process_new(n) :
    try:
        files = os.listdir("../docker/data/boursorama/2021")
        for file in files[:563]:
            store_file_new(file, "boursorama",n)
    except Exception as e:
        print("Exception occurred:", e)
        print(datetime.now(timezone.utc))
        exit(1) 
if __name__ == '__main__':
    process_new(10000000)
    #store_file_old("amsterdam 2019-01-01 090502.607291.bz2", "boursorama")
   
        method_old = timeit.timeit(lambda: store_file_old("amsterdam 2019-01-01 090502.607291.bz2", "boursorama"), number=100)
        method_new = timeit.timeit(lambda: store_file_new("amsterdam 2019-01-01 090502.607291.bz2", "boursorama"), number=100)
   
    method_old = timeit.timeit(lambda: process_new(1000), number=1)
    method_new = timeit.timeit(lambda: process_new(1000), number =1)

    # Print the results
    print("method_old Time: 1000 / ", method_old)
    print("method_new Time: 1000 / ", method_new)

    method_old = timeit.timeit(lambda: process_new(10000000), number=1)
    method_new = timeit.timeit(lambda: process_new(10000000), number =1)

    # Print the results
    print("method_old Time: 1000 / ", method_old)
    print("method_new Time: 1000 / ", method_new)
    # Print the number of files in the directory ../docker/data/boursorama/2021
    #print(len(os.listdir("../docker/data/boursorama/2021")))
    
     #2021 2022 2023 Y4A DU PEA PME
     # de ce que j'ai vu il existe seulement compA compB amsterdam et peapme ... pme que pour 2021 2022 2023
      """