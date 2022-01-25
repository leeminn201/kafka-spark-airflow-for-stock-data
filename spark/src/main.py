import streamlit as st
import yfinance as yf
import datetime
import numpy as np
import requests
from bs4 import BeautifulSoup as bs
import time
from kafka import KafkaConsumer
import json
import pandas as pd
import altair as alt

#---------------------------------------------------------------------------------- Function ---------------------------------------------------------------------------------------------

# # Get 500 american stock tickers from wikipedia
# def get_snp500_tickers_from_wiki():
#     response = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
#     soup = bs(response.text)
#     symbol_list = soup.select('table')[0].select('tr')[1:]  # get first table and select <tr> [1:end]
#     symbols = []
#     for i, symbol in enumerate(symbol_list):
#         tds = symbol.select('td')
#         symbols.append(tds[0].select('a')[0].text)
#     return symbols

# symbols = get_snp500_tickers_from_wiki()
# symbols = symbols[:len(symbols) // 25]  # select 25 ticker

#List the most popular stock 
symbols = ['AAPL', 'ADBE', 'AMZN', 'FB', 'GOOG', 'NFLX', 'TSLA', 'TWTR', 'UBER', 'V']

# Get data
def get_data_raw(tickers):
    data = []
    for ticker in tickers:
        data.append(yf.download(ticker, period='1y', interval='1d').reset_index())
    return data

# data_historical = get_data_raw(symbols)


# Get date/datetime of a stock
def getDateStock(ticker):
    data = []
    data.append(yf.download(ticker, period='1y', interval='1d').reset_index())
    return data[0]['Date']

# data_realtime['Date','Close']

def get_data_realtime(tickers):
    data = []
    for ticker in tickers:
        data.append(yf.download(ticker, period='1d', interval='1m').reset_index())
    return data

data_realtime = yf.download('AAPL', period='1d', interval='1m')


# -----------------------------------------------------------------------------------------------------------------

st.header('PICK A STOCK')

StockCode = st.selectbox(label='STOCK CODE', options=symbols)

a = getDateStock(StockCode)

st.header('OVERVIEW STOCK')

overviewStock = yf.download(StockCode, period='1y', interval='1d')
# overviewStock
import plotly.graph_objects as go

fig = go.Figure()
fig.add_trace(go.Candlestick(x=a,
                            high=overviewStock['High'],
                             low=overviewStock['Low'],
                             close=overviewStock['Close'],
                             open=overviewStock['Open']))
fig.update_layout(
    title= StockCode + ' Stock in 12 months', width = 1000, height = 500
)
col1, col2 = st.columns(2)
overviewChart = col1.plotly_chart(fig)
per = str(round((round(overviewStock['Close'][-1], ndigits=5)/round(overviewStock['Close'][-2], ndigits=5)) - 1, ndigits=5))
col2.metric(StockCode+' Today', round(overviewStock['Close'][-1], ndigits=5), per+" %")

# Kafka Consumer recieve data from kafka producer 
consumer = KafkaConsumer(bootstrap_servers=["kafka"],value_deserializer=lambda x: json.loads(x.decode("ascii")))
consumer.subscribe(topics="web_data")

# st.warning('Successful')
st.header('REALTIME STOCK CHART')
# last_row = pd.DataFrame({'Price':[data_realtime['Close']], 'Time':[data_realtime[-1]['Datetime'].iloc[-1]]})
chart = st.line_chart()

for message in consumer:

    x = message.value
    # time = datetime.datetime.strptime(x['time'], '%Y-%m-%d %H:%M:%S')
    # .time()
    df = pd.DataFrame({'Price':[x[StockCode]], 'Time':[x['time']]})
    # df.reset_index(drop=True)
    df = df.set_index('Time')
    # status_text.text("%i%% Complete" % i)
    chart.add_rows(df)
