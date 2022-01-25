import pandas as pd
from time import sleep
import yfinance as yf
import datetime as dt
import numpy as np
from kafka import KafkaProducer
import json



def get_api_data():
    #Download symbols data from yahoo finance
    symbols = ['FB','AAPL','GOOG','AMZN','TSLA','TWTR','NFLX','UBER','ADBE','V']
    yesterday = (dt.datetime.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    df = pd.DataFrame(yf.download(symbols,start_date = yesterday, end_date = yesterday,period='1d', interval='1m')).reset_index()
    return df.fillna(method="bfill")


def close_low_high(df):
    #Groupby dataframe by datetime. Then get columns high, low, close
    #Each row of close will contain "n" symbols columns. [Datetime  FB's Close GOOG's Close  ...'s Close]
    #Same as high, low
    
    temp = df.groupby("Datetime")
    close = pd.DataFrame()
    high = pd.DataFrame()
    low = pd.DataFrame()
    for key, group in temp:
        close = close.append(group["Close"])
        high = high.append(group["High"])
        low = low.append(group["Low"])
    return close, low, high
 
def produce_row(close, low, high, col):
    #Random to create more data of 1 symbol cause we only have 1 minutes data, I wanna make it more realistic, like realtime data
    #Add 5 more row per minutes
    #Example: Between 9:30 and 9:31, I will create 5 row, represent for 9h30 10s, 9h30 20s ...
    #The values of 9h30 -> 9h31 will be random between low and high price of 9h31
    df = pd.DataFrame(columns=[col])
    for i in range(len(close) - 1):
        row = np.random.uniform(low = low[i+1], high = high[i+1], size=5)
        temp = np.append(close[i], row)
        df = df.append(pd.DataFrame(temp, columns=[col]),ignore_index=True)
    df = df.append(pd.DataFrame([close.iloc[-1]], columns=[col]), ignore_index=True)
    return pd.DataFrame(df)

def merge_symbol(close, low, high):
    #Merge all symbols to a dataframe
    df = pd.DataFrame()
    for col in range(len(close.columns)):
            temp = produce_row(close.iloc[:,col], low.iloc[:,col], high.iloc[:,col], close.columns[col])
            df = df.merge(temp, left_index=True, right_index=True, how="right")
    return df

def produce_data():
    df = get_api_data()
    close, low, high = close_low_high(df)
    df = merge_symbol(close, low, high)
    yesterday = (dt.datetime.today() - dt.timedelta(days=1)).date()
    time = dt.datetime(year=yesterday.year, month=yesterday.month, day=yesterday.day, hour=9, minute=30)
    list_index = [time + dt.timedelta(seconds=10*x) for x in range(len(df))]
    df['Datetime'] = list_index
    df.set_index("Datetime",inplace=True)
    
    return df


class StdoutListener():
    def __init__(self, producer, topic_name):
        self.producer = producer
        self.topic_name = topic_name
    def on_data(self,data):

        for key, row in data.iterrows():
            d = dict(row)
            d['time'] = str(key) 
            self.producer.send(self.topic_name,d)
            sleep(10)


    
