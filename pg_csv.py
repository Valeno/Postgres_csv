import pandas as pd
import psycopg2
from api_secrets import pg
from sys import exit as ext
from os import listdir as lstdir 
from os import path

convert_type = {
    'object': 'varchar',
    'float64': 'float',
    'int64': 'int',
    'datetime64': 'timestamp',
    'timedelta64[ns]': 'varchar'
}

def col_str(cols, replacements):
    col_str = ', '.join("{} {}".format(n, d.upper()) for (n, d) in zip(cols, df.dtypes.replace(replacements)))
    return col_str

con = psycopg2.connect(host='localhost', port='5432', database='test', user='postgres', password=pg)

try:
    con
    cur = con.cursor()
    print('connection succesful')
except:
    print('could not connect to server')

PATH = r"C:\Users\dump"
print("1.) Mass dump \n q quit")
print('files in folder: ', lstdir(PATH))
usr = input('')
if usr == '1':
    print('Mass dump')
    for file in lstdir(PATH):
        df = pd.read_csv(path.join(PATH, file))
        df_col = []
        renamed = {}
        for cols in df.columns:
            df_col.append(cols)
        changed_col = [i.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('$', '').replace('%', '').replace('#', '') #maybe rename
                        .replace('!', '').replace('-', '_').replace('/', '_') for i in df_col]
        for idx, i in enumerate(df_col):
            renamed.update({i: changed_col[idx]})
        df = df.rename(columns=renamed)
        col_create = col_str(df.columns, convert_type)
        df.to_csv(path.join(PATH, file), index=False, mode='w+')
        create_table = f"CREATE TABLE IF NOT EXISTS {path.splitext(file)[0]} ({col_create});"
        cur.execute(create_table)
        print(create_table)
        fo = open(path.join(PATH, file))
        SQL = f"""
        COPY {path.splitext(file)[0]} FROM STDIN WITH 
        CSV
        HEADER
        DELIMITER AS ','
        """     
        cur.copy_expert(sql=SQL, file=fo)
        fo.close()
        print(SQL)
elif usr == 'q':
    print('Closing connection')
    con.close()
    ext()

con.commit()
con.close()
print('Connection closed')
waitt = input('')

