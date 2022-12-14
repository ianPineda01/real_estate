import pandas as pd

from typing import Optional
import sqlite3

def write_DF(DF:pd.DataFrame, db_name:str):
    """
    Takes a spark dataframe and writes it's contents into a sqlite database
    """
    with sqlite3.connect(db_name) as conn:
        DF.to_sql('units', conn, if_exists='append', index=False)
    
def clear_data(db_name:str):
    """
    Clears all the data from database
    """
    with sqlite3.connect(db_name) as conn:
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS units')
        

def read_to_DF(db_name:str) -> Optional[pd.DataFrame]:
    """
    Reads the contents of a database and writes them into a spark dataframe
    """
    with sqlite3.connect(db_name) as conn:
        try:
            df = pd.read_sql_query('SELECT * FROM units', conn)
        except pd.errors.DatabaseError:
            return None

    return df