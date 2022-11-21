from pyspark.sql import SparkSession, DataFrame
import pandas as pd

import sqlite3

def write_DF(DF:DataFrame, db_name:str):
    """
    Takes a spark dataframe and writes it's contents into a sqlite database
    """
    with sqlite3.connect(db_name) as conn:
        DF.toPandas().to_sql('units', conn)

def read_to_DF(spark: SparkSession, db_name:str) -> DataFrame:
    """
    Reads the contents of a database and writes them into a spark dataframe
    """
    with sqlite3.connect(db_name) as conn:
        df = pd.read_sql_query('SELECT * FROM units', conn)

    return spark.createDataFrame(df)