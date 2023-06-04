import streamlit as st
import pyodbc
import pandas as pd
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
        SELECT
            DATEPART(HOUR, timestamp) as hour,
            price,
            SUM(CASE WHEN timestamp >= DATEADD(MINUTE, -5, GETDATE()) THEN volume ELSE 0 END) AS past_5min_volume,
            SUM(CASE WHEN timestamp >= DATEADD(MINUTE, -15, GETDATE()) THEN volume ELSE 0 END) AS past_15min_volume,
            SUM(CASE WHEN timestamp >= DATEADD(MINUTE, -60, GETDATE()) THEN volume ELSE 0 END) AS past_60min_volume
        FROM {coin}usdt
        WHERE timestamp >= DATEADD(MINUTE, -60, GETDATE())
        GROUP BY DATEPART(HOUR, timestamp), price
    """
    # ...


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

if __name__ == '__main__':
    main()
