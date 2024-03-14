import pandas as pd
import numpy as np
import sklearn
import os
import timescaledb_model as tsdb

#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def store_file(name, website):
    """
    if db.is_file_done(name):
        return
    """
    if website.lower() == "boursorama":
        try:
            df = pd.read_pickle("/docker/data/boursorama/" + name)  # is this dir ok for you ?
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("../docker/data/boursorama/" + year + "/" + name)
        # to be finished
    #Print the companies table
    request = db.get_companies()
    for r in request:
        print(r)
    
if __name__ == '__main__':
    store_file("amsterdam 2019-01-01 090502.607291.bz2", "boursorama")
    #store_file("test.txt", "boursorama")
    print("Done")
# amsterdam 2019-01-01 090502.607291.bz2