import streamlit as st
import pyodbc
import pandas as pd
import time 
from datetime import datetime, timedelta

def get_db_connection():
    connection = pyodbc.connect(
        driver='{ODBC Driver 17 for SQL Server}',
        server='A2NWPLSK14SQL-v06.shr.prod.iad2.secureserver.net',
        database='db_ran',
        uid='dbahsantrade',
        pwd='Pak@1947'
    )
    return connection

def fetch_data(coin):
    query = f"""
        SELECT price,
            SUM(CASE WHEN timestamp >= DATEADD(MINUTE, DATEDIFF(MINUTE, 0, GETDATE()) / 300 * 300, 0) AND timestamp < DATEADD(MINUTE, DATEDIFF(MINUTE, 0, GETDATE()) / 300 * 300 + 5, 0) THEN volume ELSE 0 END) AS '5m',
            SUM(CASE WHEN timestamp >= DATEADD(MINUTE, DATEDIFF(MINUTE, 0, GETDATE()) / 300 * 300 - 5, 0) AND timestamp < DATEADD(MINUTE, DATEDIFF(MINUTE, 0, GETDATE()) / 300 * 300, 0) THEN volume ELSE 0 END) AS '5m_before',
            SUM(CASE WHEN timestamp >= DATEADD(MINUTE, DATEDIFF(MINUTE, 0, GETDATE()) / 900 * 900, 0) AND timestamp < DATEADD(MINUTE, DATEDIFF(MINUTE, 0, GETDATE()) / 900 * 900 + 15, 0) THEN volume ELSE 0 END) AS '15m',
            SUM(CASE WHEN timestamp >= DATEADD(MINUTE, DATEDIFF(MINUTE, 0, GETDATE()) / 900 * 900 - 15, 0) AND timestamp < DATEADD(MINUTE, DATEDIFF(MINUTE, 0, GETDATE()) / 900 * 900, 0) THEN volume ELSE 0 END) AS '15m_before',
            SUM(CASE WHEN timestamp >= DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0) AND timestamp < DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()) + 1, 0) THEN volume ELSE 0 END) AS '60m',
            SUM(CASE WHEN timestamp >= DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()) - 1, 0) AND timestamp < DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0) THEN volume ELSE 0 END) AS '60m_before'
        FROM {coin}usdt
        WHERE timestamp >= CAST(GETDATE() AS DATE)
        GROUP BY price
    """

    connection = get_db_connection()
    df = pd.read_sql_query(query, connection)
    connection.close()

    return df
def fetch_daily_data(coin, selected_date):
    query = f"""
        SELECT price,
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '00:00:00' AND '03:59:59' THEN volume ELSE 0 END) AS '4h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '04:00:00' AND '07:59:59' THEN volume ELSE 0 END) AS '8h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '08:00:00' AND '11:59:59' THEN volume ELSE 0 END) AS '12h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '12:00:00' AND '15:59:59' THEN volume ELSE 0 END) AS '16h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '16:00:00' AND '19:59:59' THEN volume ELSE 0 END) AS '20h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '20:00:00' AND '23:59:59' THEN volume ELSE 0 END) AS '24h'
        FROM {coin}usdt
        WHERE CAST(timestamp AS DATE) = '{selected_date}'
        GROUP BY price
    """

    connection = get_db_connection()
    df = pd.read_sql_query(query, connection)
    connection.close()

    return df
def main():
    # Set Streamlit app title and layout
    st.title("Cryptocurrency Market Data")
    st.write("Market data retrieved from SQL Server")

    # Get the list of coins
    coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil", "cfx", "adx", "chz", "bel", "alpaca", "elf", "epx", "pros", "t", "dar", "agix", "mob", "id", "trx", "key", "tru", "amb", "magic", "lina", "lever"]


    # Create a selectbox for coin selection
    selected_coin = st.selectbox("Select a coin", coins)

    # Fetch trading data for the selected coin
    df = fetch_data(selected_coin)

    # Display the data
    st.dataframe(df)

    # Get dates for the last 7 days
    dates = [(datetime.now() - timedelta(days=i)).date() for i in range(7)]

    # Create a selectbox for date selection
    selected_date = st.selectbox("Select a date", dates)

    # Fetch daily data for the selected coin
    daily_df = fetch_daily_data(selected_coin, selected_date)

    # Display the daily data
    st.dataframe(daily_df)

if __name__ == '__main__':
    main()
