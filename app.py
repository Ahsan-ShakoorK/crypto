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
        SELECT ROUND(price, 6) AS price,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/300)*300) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/300)*300 + 300) THEN volume ELSE 0 END) AS volume_5min,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/300)*300 - 300) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/300)*300) THEN volume ELSE 0 END) AS volume_5min_before,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/900)*900) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/900)*900 + 900) THEN volume ELSE 0 END) AS volume_15min,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/900)*900 - 900) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/900)*900) THEN volume ELSE 0 END) AS volume_15min_before,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/3600)*3600) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/3600)*3600 + 3600) THEN volume ELSE 0 END) AS volume_60min,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/3600)*3600 - 3600) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/3600)*3600) THEN volume ELSE 0 END) AS volume_60min_before
    FROM {coin}usdt
    WHERE timestamp >= CURDATE() + INTERVAL 1 SECOND
    GROUP BY priceq
    """

    connection = get_db_connection()
    df = pd.read_sql_query(query, connection)
    connection.close()

    return df

def fetch_daily_data(coin, selected_date):
    query = f"""
        SELECT price,
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '00:00:00' AND '00:59:59' THEN volume ELSE 0 END) AS '1h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '01:00:00' AND '01:59:59' THEN volume ELSE 0 END) AS '2h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '02:00:00' AND '02:59:59' THEN volume ELSE 0 END) AS '3h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '03:00:00' AND '03:59:59' THEN volume ELSE 0 END) AS '4h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '04:00:00' AND '04:59:59' THEN volume ELSE 0 END) AS '5h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '05:00:00' AND '05:59:59' THEN volume ELSE 0 END) AS '6h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '06:00:00' AND '06:59:59' THEN volume ELSE 0 END) AS '7h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '07:00:00' AND '07:59:59' THEN volume ELSE 0 END) AS '8h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '08:00:00' AND '08:59:59' THEN volume ELSE 0 END) AS '9h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '09:00:00' AND '09:59:59' THEN volume ELSE 0 END) AS '10h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '10:00:00' AND '10:59:59' THEN volume ELSE 0 END) AS '11h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '11:00:00' AND '11:59:59' THEN volume ELSE 0 END) AS '12h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '12:00:00' AND '12:59:59' THEN volume ELSE 0 END) AS '13h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '13:00:00' AND '13:59:59' THEN volume ELSE 0 END) AS '14h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '14:00:00' AND '14:59:59' THEN volume ELSE 0 END) AS '15h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '15:00:00' AND '15:59:59' THEN volume ELSE 0 END) AS '16h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '16:00:00' AND '16:59:59' THEN volume ELSE 0 END) AS '17h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '17:00:00' AND '17:59:59' THEN volume ELSE 0 END) AS '18h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '18:00:00' AND '18:59:59' THEN volume ELSE 0 END) AS '19h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '19:00:00' AND '19:59:59' THEN volume ELSE 0 END) AS '20h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '20:00:00' AND '20:59:59' THEN volume ELSE 0 END) AS '21h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '21:00:00' AND '21:59:59' THEN volume ELSE 0 END) AS '22h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '22:00:00' AND '22:59:59' THEN volume ELSE 0 END) AS '23h',
            SUM(CASE WHEN CAST(timestamp AS TIME) BETWEEN '23:00:00' AND '23:59:59' THEN volume ELSE 0 END) AS '24h'
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
