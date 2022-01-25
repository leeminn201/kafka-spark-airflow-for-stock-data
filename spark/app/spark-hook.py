import sys
import pandas as pd
import yfinance as yf
import datetime as dt
from pyspark.sql import SparkSession



spark = (SparkSession
    .builder
    .getOrCreate()
)

####################################
# Parameters
####################################
postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]
####################################

from pyspark.sql.types import StructType,StructField, DoubleType, LongType, StringType, DateType
schema = StructType([
  StructField('Date', DateType(), True),
  StructField('Open', DoubleType(), True),
  StructField('High', DoubleType(), True),
  StructField('Low', DoubleType(), True),
  StructField('Close', DoubleType(), True),
  StructField('Adj_Close', DoubleType(), True),
  StructField('Volume', LongType(), True),
  StructField('Symbol', StringType(), True),
  ])

def _fetch_data(schema):
    payload=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df_symbol = payload[0] #get 500 symbol
    symbols = df_symbol['Symbol'].values.tolist()[:200]
    symbols.remove('BF.B')
    symbols.remove('BRK.B')
    
    df = spark.createDataFrame([],schema)
    for symbol in symbols:
        try:
            temp = pd.DataFrame(yf.download(symbol, period='1d', interval='1d')).reset_index()
            temp['Symbol'] = symbol
            temp = spark.createDataFrame(temp)
            df = df.union(temp)
        except: continue
            
    return df

df = _fetch_data(schema)

(
    df.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.stock")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)
