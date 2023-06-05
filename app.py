import streamlit as st
import pymssql
import pandas as pd
import time
import datetime

# Establish connection to SQL Server
connection = pymssql.connect(
    server='A2NWPLSK14SQL-v06.shr.prod.iad2.secureserver.net',
    user='dbahsantrade',
    password='Pak@1947',
    database='db_ran'
)

def fetch_trading_data(coin):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
        SELECT ROUND(price, 6) AS price,
            SUM(CASE WHEN timestamp >= DATEADD(MINUTE, -5, '{current_time}') AND timestamp < '{current_time}' THEN volume ELSE 0 END) AS volume_5min,
            SUM(CASE WHEN timestamp >= DATEADD(MINUTE, -10, '{current_time}') AND timestamp < DATEADD(MINUTE, -5, '{current_time}') THEN volume ELSE 0 END) AS volume_5min_before,
            SUM(CASE WHEN timestamp >= DATEADD(MINUTE, -15, '{current_time}') AND timestamp < '{current_time}' THEN volume ELSE 0 END) AS volume_15min,
            SUM(CASE WHEN timestamp >= DATEADD(MINUTE, -30, '{current_time}') AND timestamp < DATEADD(MINUTE, -15, '{current_time}') THEN volume ELSE 0 END) AS volume_15min_before,
            SUM(CASE WHEN timestamp >= DATEADD(HOUR, -1, '{current_time}') AND timestamp < '{current_time}' THEN volume ELSE 0 END) AS volume_60min,
            SUM(CASE WHEN timestamp >= DATEADD(HOUR, -2, '{current_time}') AND timestamp < DATEADD(HOUR, -1, '{current_time}') THEN volume ELSE 0 END) AS volume_60min_before
        FROM {coin}usdt
        WHERE timestamp >= CAST(GETDATE() AS DATE)
        GROUP BY price
    """

    # Rest of the code...


    # Rest of the code...


    with connection.cursor(as_dict=True) as cursor:
        cursor.execute(query)
        data = cursor.fetchall()

    # Filter out rows where all volume columns are 0
    df = pd.DataFrame(data)
    volume_columns = ['volume_5min', 'volume_5min_before', 'volume_15min', 'volume_15min_before', 'volume_60min', 'volume_60min_before']
    df = df.loc[~(df[volume_columns] == 0).all(axis=1)]
    df = df.rename(columns={
        'volume_5min': '5m',
        'volume_5min_before': '5m_b',
        'volume_15min': '15m',
        'volume_15min_before': '15m_b',
        'volume_60min': '60m',
        'volume_60min_before': '60m_b'
    })
    return df

def main():
    # Set Streamlit app title and layout
    st.title("Cryptocurrency Market Trading Data")
    st.write("Market data retrieved from SQL Server")

    # Get the list of coins
    coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil"]

    # Create a selectbox for coin selection
    selected_coin = st.selectbox("Select a coin", coins)

    # Fetch trading data for the selected coin
    df = fetch_trading_data(selected_coin)
    st.write(df)  # Display the DataFrame

    current_time = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
    st.write(f"Current Time: {current_time}")

    time.sleep(5)
    st.experimental_rerun()

if __name__ == '__main__':
    main()
