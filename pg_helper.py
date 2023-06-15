
import pandas as pd
import psycopg2
from api_secrets import pg_pw
from os import listdir 
from os import path
from sqlalchemy import create_engine
from sqlalchemy import __version__ as alchv
import sqlite3


class Bridge:
    PG_PW = pg_pw
    PG_PORT = '5432'
    PG_DB = 'test'
    PG_USER = 'postgres'
    PG_HOST = 'localhost'

    def __init__(self, db: str | None=PG_DB, username: str | None=PG_USER,
                password: str | None=PG_PW, port: str | None=PG_PORT, host: str | None=PG_HOST):
        self.db = db
        self.username = username
        self.password = password
        self.port = port
        self.host = host

    def psyco_con(self):
        try:    
            con = psycopg2.connect(host=self.host, port=self.port, database=self.db, user=self.username, password=self.password)
            return con
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def alch_con(self):
        if alchv != '1.4.46':
            print('SQLAlchemy version may not be compatible with import tools')
            pass
        else:
            con = f'postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.db}'
            alch = create_engine(con)
            return alch.connect()
        
    @classmethod
    def lite_con(cls, db: str | None=PG_DB):
        cls.db = db
        con = sqlite3.connect(database=cls.db)
        return con


class CsvTools: #ImportTools
    LOC = 'dump'
    def __init__(self, location: str=None):
        if location is None:
            self.location = self.LOC
        else:
            self.location = location # abs_location = path.abspath(self.location)
           
    def files_in_location(self):
        """Return names of all files in location specified"""
        for idx, file in enumerate(listdir(self.location)):
            print(f"{idx}.) {file}")

       
    def mass_dump(self, db=None):                                    
        """Only working with SQLAlchemy version 1.4.46, SQLAlchemy2 is not yet supported by Pandas for postgres"""
        if db is None:               
            pass
        else:
            for idx, file in enumerate(listdir(self.location)): 
                alchemy_engine = Bridge(db).alch_con()       
                df = pd.read_csv(path.join(path.abspath(self.location), listdir(path.abspath(self.location))[idx]))
                col_check = CsvTools.column_clean(df)
                df = df.rename(columns=col_check)
                df.to_sql(path.splitext(file)[0], con=alchemy_engine, index=False, if_exists='replace')   

    def custom_dump(self, db=None): 
        if db is None:
            pass
        else:
            sql_alch = Bridge(db).alch_con()
            self.files_in_location()
            usr = input('Select the file: ')
            selection = listdir(self.location)[int(usr)]
            df = pd.read_csv(self.location + "/" + selection)
            col_check = CsvTools.column_clean(df)
            df.rename(columns=col_check, inplace=True)
            df.to_sql(selection.split('.')[0], sql_alch, index=False, if_exists='replace') 

    def from_link(self):
        pass

    @staticmethod
    def column_clean(data):
        """
        Format column names from pandas dataframe for compatability.
        Returns a dictionary of changed values.
        """
        existing_cols = []
        rename = {}
        for i in data.columns:
            existing_cols.append(i)
            new_cols = [i.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('$', '').replace('%', '').replace('#', '')
                        .replace('!', '').replace('-', '_').replace('/', '_').replace('*', '') for i in existing_cols]
        for idx, i in enumerate(existing_cols):
            rename.update({i: new_cols[idx]})
        return rename

    def __repr__(self):
        return print(f'{self.__class__.__name__}, file location = {self.location}')
    
