import pandas as pd
import sys
from ..analyzer.timescaledb_model import db

def load_data():
    return db.df_query('SELECT * FROM select * from daystocks where cid = 2')
    return pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv')

dataframe_finances = load_data()
print(dataframe_finances.head())