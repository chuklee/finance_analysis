# -*- coding: utf-8 -*-

# TimeScaleDB
# pipenv install sqlalchemy-timescaledb

import datetime
import psycopg2
import pandas as pd
import psycopg2.pool
import sqlalchemy
import os

import mylogging

from io import StringIO


class TimescaleStockMarketModel:
    """ Bourse model with TimeScaleDB persistence."""

    def __init__(self, database, user=None, host=None, password=None, port=None):
        """Create a TimescaleStockMarketModel
        database -- The name of the persistence database.
        user     -- Username to connect with to the database. Same as the
                    database name by default.
        """
        self.logger = mylogging.getLogger(__name__, filename="/tmp/bourse.log")
        self.__database = database
        self.__user = user or database
        self.__host = host or 'localhost'
        self.__port = port or 5432
        self.__password = password or ''
        self.__squash = False
        self.__connection = psycopg2.connect(database=self.__database,
                                             user=self.__user,
                                             host=self.__host,
                                             password=self.__password)
        self.__engine = sqlalchemy.create_engine(f'timescaledb://{self.__user}:{self.__password}@{self.__host}:{self.__port}/{self.__database}')
        self.__nf_cid = {}  # cid from netfonds symbol
        self.__boursorama_cid = {}  # cid from netfonds symbol
        self.__market_id = {}  # id of markets from aliases
        self.logger.info("Setup database generates an error if it exists already, it's ok")
        self._setup_database()

    """ def setup_connection(self):
        # Setup a new database connection
        self.logger = mylogging.getLogger(__name__, filename="/tmp/bourse.log")
        self.__connection = psycopg2.connect(database=self.__database,
                                             user=self.__user,
                                             host=self.__host,
                                             password=self.__password)
        self.__engine = sqlalchemy.create_engine(f'timescaledb://{self.__user}:{self.__password}@{self.__host}:{self.__port}/{self.__database}',  pool_size=20, max_overflow=0) """
    

    def _setup_database(self):
        try:
            # Create the tables if they do not exist.
            #
            # To drop all tables (clean) do
            #   drop schema public cascade;
            #   create schema public;
            #
            cursor = self.__connection.cursor()
            # markets (see end for list of makets)
            cursor.execute('''CREATE SEQUENCE market_id_seq START 1;''')
            cursor.execute(
                '''CREATE TABLE markets (
                  id SMALLINT PRIMARY KEY DEFAULT nextval('market_id_seq'),
                  name VARCHAR,
                  alias VARCHAR
                );''')
            # company:
            #   - mid : market id
            #
            cursor.execute('''CREATE SEQUENCE company_id_seq START 1;''')
            cursor.execute(
                '''CREATE TABLE companies (
                  id SMALLINT PRIMARY KEY DEFAULT nextval('company_id_seq'),
                  name VARCHAR,
                  mid SMALLINT,
                  symbol VARCHAR,
                  symbol_nf VARCHAR,
                  isin CHAR(12),
                  reuters VARCHAR,
                  boursorama VARCHAR,
                  pea BOOLEAN,
                  sector INTEGER
                );''')
            cursor.execute(
                '''CREATE TABLE stocks (
                  date TIMESTAMPTZ,
                  cid SMALLINT,
                  value FLOAT4,
                  volume INT
                );''')
            #cursor.execute("ALTER TABLE stocks ADD COLUMN name VARCHAR, ADD COLUMN pea BOOLEAN, ADD COLUMN mid SMALLINT, ADD COLUMN symbol VARCHAR;")
            
            cursor.execute('''SELECT create_hypertable('stocks', by_range('date'));''')
            cursor.execute('''CREATE INDEX idx_cid_stocks ON stocks (cid, date DESC);''')
            cursor.execute(
                '''CREATE TABLE daystocks (
                  date TIMESTAMPTZ,
                  cid SMALLINT,
                  open FLOAT4,
                  close FLOAT4,
                  high FLOAT4,
                  low FLOAT4,
                  volume INT
                );''')
            cursor.execute('''SELECT create_hypertable('daystocks', by_range('date'));''')
            cursor.execute('''CREATE INDEX idx_cid_daystocks ON daystocks (cid, date DESC);''')
            cursor.execute(
                '''CREATE TABLE file_done (
                  name VARCHAR PRIMARY KEY
                );''')
            cursor.execute(
                '''CREATE TABLE tags (
                  name VARCHAR PRIMARY KEY,
                  value VARCHAR
                );''')
            # let insert known market
            cursor.execute("INSERT INTO markets (id, name, alias) VALUES (1,'NYSE Euronext','euronx');")
            cursor.execute("INSERT INTO markets (id, name, alias) VALUES (2,'London Stock Exchange','lse');")
            cursor.execute("INSERT INTO markets (id, name, alias) VALUES (3,'Bourse Italienne','milano');")
            cursor.execute("INSERT INTO markets (id, name, alias) VALUES (4,'Bourse Allemande','dbx');")
            cursor.execute("INSERT INTO markets (id, name, alias) VALUES (5,'Bourse Espagnole','mercados');")
            cursor.execute("INSERT INTO markets (id, name, alias) VALUES (6,'Amsterdam','amsterdam');")
            cursor.execute("INSERT INTO markets (id, name, alias) VALUES (7,'Paris compartiment A','compA');")
            cursor.execute("INSERT INTO markets (id, name, alias) VALUES (8,'Paris compartiment B','compB');")
            cursor.execute("INSERT INTO markets (id, name, alias) VALUES (9,'Bourse Allemande','xetra');")
            cursor.execute("INSERT INTO markets (id, name, alias) VALUES (10,'Bruxelle','bruxelle');")

        except Exception as e:
            self.logger.exception('SQL error: %s' % e)
        self.__connection.commit()

    # ------------------------------ public methods --------------------------------

    def execute(self, query, args=None, cursor=None, commit=False):
        """Send a Postgres SQL command. No return"""
        if args is None:
            pretty = query
        else:
            pretty = '%s %% %r' % (query, args)
        self.logger.debug('SQL: QUERY: %s' % pretty)
        if cursor is None:
            cursor = self.__connection.cursor()
        cursor.execute(query, args)
        if commit:
            self.commit()
        try:
            return cursor.fetchall()
        except:
            pass

    def df_write(self, df, table, args=None, commit=False,
                 if_exists='append', index=True, index_label=None,
                 chunksize=1000, dtype=None, method="multi"):
        '''Write a Pandas dataframe to the Postgres SQL database

        :param query:
        :param args: arguments for the query
        :param commit: do a commit after writing
        :param other args: see https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_sql.html
        '''
        self.logger.debug('df_write')
        df.to_sql(table, self.__engine,
                if_exists=if_exists, index=index, index_label=index_label,
                chunksize=chunksize, dtype=dtype, method=method)
        if commit:
            self.commit()
    # general query methods

    def raw_query(self, query, args=None, cursor=None):
        #Return a tuple from a Postgres SQL query
        if args is None:
            pretty = query
        else:
            pretty = '%s %% %r' % (query, args)
        self.logger.debug('SQL: QUERY: %s' % pretty)
        if cursor is None:
            cursor = self.__connection.cursor()
        cursor.execute(query, args)
        #if query.strip().upper().startswith('SELECT'):
        return cursor.fetchall()
    
    
    def df_query(self, query, args=None, index_col=None, coerce_float=True, params=None, 
                 parse_dates=None, columns=None, chunksize=1000, dtype=None):
        '''Returns a Pandas dataframe from a Postgres SQL query

        :param query:
        :param args: arguments for the query
        :param other args: see https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql.html
        :return: a dataframe
        '''
        if args is not None:
            query = query % args
        self.logger.debug('df_query: %s' % query)
        return pd.read_sql(query, self.__engine, index_col=index_col, coerce_float=coerce_float, 
                           params=params, parse_dates=parse_dates, columns=columns, 
                           chunksize=chunksize, dtype=dtype)

    # system methods

    def commit(self):
        if not self.__squash:
            self.__connection.commit()

    # write here your methods which SQL requests

    def search_company_id(self, name, getmax=1, strict=False):
        if getmax > 1:
            res = self.raw_query('SELECT (id) FROM companies WHERE LOWER(name) LIKE LOWER(%s)',
                                 ('%' + name + '%',))
        else:
            res = self.raw_query('SELECT (id) FROM companies WHERE name = %s', (name,))
            if len(res) == 0 and not strict:
                res = self.raw_query('SELECT (id) FROM companies WHERE LOWER(name) LIKE LOWER(%s)', (name,))
                if len(res) == 0:
                    res = self.raw_query('SELECT (id) FROM companies WHERE name LIKE %s', (name + '%',))
                    if len(res) == 0:
                        res = self.raw_query('SELECT (id) FROM companies WHERE name LIKE %s', ('%' + name + '%',))
                        if len(res) == 0:
                            res = self.raw_query('SELECT (id) FROM companies WHERE LOWER(name) LIKE LOWER(%s)',
                                                 ('%' + name + '%',))
        if len(res) == 1:
            return res[0][0]
        elif len(res) > 1 and len(res) < getmax:
            return [r[0] for r in res]
        else:
            return 0
        
    
    def insert_companies(self, name, pea, mid, symbol, commit=True):
        cursor = self.__connection.cursor()
        cursor.execute("INSERT INTO companies (name, pea, mid, symbol) VALUES (%s, %s, %s, %s) RETURNING id;", (name, pea, mid, symbol))
        result = cursor.fetchone()
        if result is not None:
            new_id = result[0]  # Fetch the ID of the newly added row
            if commit:
                self.commit()
            return new_id
        else:
            self.logger.warning("No ID returned after inserting company: %s, the fetchone method returned None", name)
            return None


    def search_company_id_by_symbol(self, name, getmax=1, strict=False):
        if getmax > 1:
            res = self.raw_query('SELECT (id) FROM companies WHERE LOWER(symbol) LIKE LOWER(%s)',
                                 ('%' + name + '%',))
        else:
            res = self.raw_query('SELECT (id) FROM companies WHERE symbol = %s', (name,))
            if len(res) == 0 and not strict:
                res = self.raw_query('SELECT (id) FROM companies WHERE LOWER(symbol) LIKE LOWER(%s)', (name,))
                if len(res) == 0:
                    res = self.raw_query('SELECT (id) FROM companies WHERE symbol LIKE %s', (name + '%',))
                    if len(res) == 0:
                        res = self.raw_query('SELECT (id) FROM companies WHERE symbol LIKE %s', ('%' + name + '%',))
                        if len(res) == 0:
                            res = self.raw_query('SELECT (id) FROM companies WHERE LOWER(symbol) LIKE LOWER(%s)',
                                                 ('%' + name + '%',))
        if len(res) == 1:
            return res[0][0]
        elif len(res) > 1 and len(res) < getmax:
            return [r[0] for r in res]
        else:
            return 0

    def is_file_done(self, name):
        '''
        Check if a file has already been included in the DB
        '''
        return  self.raw_query("SELECT EXISTS ( SELECT 1 FROM file_done WHERE nom = '%s' );" % name)

    def get_companies(self):
        '''
        Return a dataframe with all companies
        '''
        return self.df_query('SELECT * FROM companies')

    def set_volume_bigint(self):
        cursor = self.__connection.cursor()
        cursor.execute("ALTER TABLE stocks ALTER COLUMN volume TYPE BIGINT;")
        cursor.execute("ALTER TABLE daystocks ALTER COLUMN volume TYPE BIGINT;")
        self.commit()


    def df_write_copy(self, df, table, commit=False):
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        try:
            with self.__connection.cursor() as cursor:
                cursor.copy_from(buffer, table, sep=',', null='', columns=df.columns.tolist())
            if commit:
                self.__connection.commit()
        except Exception as e:
            self.__connection.rollback()
            if self.logger:
                self.logger.error(f"Error during db write: {e}")
            raise e
        finally:
            buffer.close()
#
# main
#

if __name__ == "__main__":
    import doctest

    # timescaleDB shoul run, possibly in Docker
    db = TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp')
    doctest.testmod()