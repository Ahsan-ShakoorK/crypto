import streamlit as st
import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta

API_KEY = '50eb99a85c56c3c3d1d4491e53f9ababce42f813793ef13da8f1dd93e7ecd920'

st.title("CryptoCompare Trading Data")

@st.cache_data
def get_coin_list():
    url = f"https://min-api.cryptocompare.com/data/all/coinlist?api_key={API_KEY}"
    response = requests.get(url)
    coin_data = json.loads(response.text)['Data']
    coins = [coin for coin in coin_data]
    return coins

def get_market_data(symbol):
    url = f"https://min-api.cryptocompare.com/data/pricemultifull?fsyms={symbol}&tsyms=USDT&api_key={API_KEY}"
    response = requests.get(url)
    data = json.loads(response.text)
    if 'RAW' in data and symbol in data['RAW'] and 'USDT' in data['RAW'][symbol]:
        market_data = data['RAW'][symbol]['USDT']
        df = pd.DataFrame.from_dict(market_data, orient='index').T
        df['lastUpdate'] = pd.to_datetime(df['LASTUPDATE'], unit='s')  # Update here
        df.set_index('lastUpdate', inplace=True)
        return df
    else:
        st.write("Error fetching market data. Please try again.")
        return None
def get_trades(df, intervals):
    trades = []
    for interval in intervals:
        end_time = df.index[-1]  # Get the last timestamp in the DataFrame
        start_time = end_time - pd.Timedelta(interval)
        if 'close' in df.columns:
            trade_data = df.loc[start_time:end_time]
            trade_column = 'close'
        elif 'open' in df.columns:
            trade_data = df.loc[start_time:end_time]
            trade_column = 'open'
        else:
            continue  # Skip this interval if neither 'close' nor 'open' columns are present
        if not trade_data.empty:  # Check if trade_data is not empty
            trade_data = trade_data.groupby(trade_column).sum()['volumeto']
            trades.append(trade_data)
        else:
            print(f"No trade data found for interval: {interval}")
            print(f"Start time: {start_time}, End time: {end_time}")
    return trades



def display_data(symbol):
    df = get_market_data(symbol)
    if df is not None:
        trades = get_trades(df, ['1T', '5T', '15T', '60T', '1D'])
        final_df = pd.concat(trades, axis=1)
        final_df.columns = ['1 Minute', '5 Minutes', '15 Minutes', '60 Minutes', '24 Hours']
        st.dataframe(final_df)
        time.sleep(15)

coin_list = get_coin_list()

selected_coin = st.text_input("Enter coin symbol (e.g., 'et' for 'ethusdt')", "BTC")

if selected_coin:
    selected_coin = selected_coin.upper()
    if selected_coin in coin_list:
        display_data(selected_coin)
    else:
        st.write("Invalid coin symbol. Please try again.")
