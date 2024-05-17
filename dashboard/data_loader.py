import pandas as pd
import sqlalchemy
DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
# DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)

def load_data(cid):
    dataframe_finances =   pd.read_sql_query('SELECT date, open, high, low, close, volume FROM public.daystocks where cid = '+cid+' order by date asc', engine)
    dataframe_finances['MA20'] = dataframe_finances['close'].rolling(window=20).mean()
    dataframe_finances['SD20'] = dataframe_finances['close'].rolling(window=20).std() 
    dataframe_finances['UpperBand'] = dataframe_finances['MA20'] + (dataframe_finances['SD20']*2)
    dataframe_finances['LowerBand'] = dataframe_finances['MA20'] - (dataframe_finances['SD20']*2)
    company_name = pd.read_sql_query(
        'SELECT name FROM public.companies WHERE id = ' + cid,
        engine
    ).iloc[0]['name'] 
    dataframe_finances['company_name'] = company_name
    return dataframe_finances