import streamlit as st
import pymssql
import pandas as pd
import time
from datetime import datetime, timedelta

# Establish connection to SQL Server
connection = pymssql.connect(
    server='A2NWPLSK14SQL-v06.shr.prod.iad2.secureserver.net',
    user='dbahsantrade',
    password='Pak@1947',
    database='db_ran'
)

def fetch_trading_data(coin):
    now = datetime.now()
    five_minute = now - timedelta(minutes=now.minute % 5, seconds=now.second, microseconds=now.microsecond)
    fifteen_minute = now - timedelta(minutes=now.minute % 15, seconds=now.second, microseconds=now.microsecond)
    one_hour = now.replace(minute=0, second=0, microsecond=0)

    query = f"""
        SELECT CAST(price AS DECIMAL(18, 5)) AS price,
            SUM(CASE WHEN timestamp >= '{five_minute}' AND timestamp < '{five_minute + timedelta(minutes=5)}' THEN volume ELSE 0 END) AS volume_5min,
            SUM(CASE WHEN timestamp >= '{five_minute - timedelta(minutes=5)}' AND timestamp < '{five_minute}' THEN volume ELSE 0 END) AS volume_5min_before,
            SUM(CASE WHEN timestamp >= '{fifteen_minute}' AND timestamp < '{fifteen_minute + timedelta(minutes=15)}' THEN volume ELSE 0 END) AS volume_15min,
            SUM(CASE WHEN timestamp >= '{fifteen_minute - timedelta(minutes=15)}' AND timestamp < '{fifteen_minute}' THEN volume ELSE 0 END) AS volume_15min_before,
            SUM(CASE WHEN timestamp >= '{one_hour}' AND timestamp < '{one_hour + timedelta(hours=1)}' THEN volume ELSE 0 END) AS volume_60min,
            SUM(CASE WHEN timestamp >= '{one_hour - timedelta(hours=1)}' AND timestamp < '{one_hour}' THEN volume ELSE 0 END) AS volume_60min_before
        FROM {coin}usdt
        WHERE timestamp >= CAST(GETDATE() AS DATE)
        GROUP BY price
    """

    with connection.cursor(as_dict=True) as cursor:
        cursor.execute(query)
        data = cursor.fetchall()

    # Filter out rows where all volume columns are 0
    df = pd.DataFrame(data)
    df = df.apply(pd.to_numeric, errors='ignore')  # Convert all columns to numeric
    df = df.sort_values('price', ascending=False)
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

    # Set the price column as the index
    df.set_index('price', inplace=True)

    # Apply styling to lock the price column
    df_styled = df.style.set_table_styles([
        {'selector': 'th:first-child', 'props': [('position', 'sticky'), ('left', '0')]},
        {'selector': 'td:first-child', 'props': [('position', 'sticky'), ('left', '0')]},
        {'selector': 'td', 'props': [('text-align', 'right')]},
    ])

    return df_styled

def fetch_daily_data(coin, selected_date, timeframe):
    intervals = {
        '5min': list(range(0, 24*60, 5)),
        '15min': list(range(0, 24*60, 15)),
        '1hour': list(range(24))
    }
    interval_list = intervals[timeframe]
    # Convert column numbers to time format

    # Convert column numbers to time format
    if timeframe == '5min' or timeframe == '15min':
        column_names = [f"{str(interval // 60).zfill(2)}:{str(interval % 60).zfill(2)}" for interval in interval_list]
    else:
        column_names = [f"{str(interval).zfill(2)}:00" for interval in interval_list]

    query = f"""
        SELECT CAST(price AS DECIMAL(18, 8)) AS price,
            {', '.join([f"SUM(CASE WHEN DATEPART(MINUTE, timestamp) = {interval} THEN volume ELSE 0 END) AS volume_{interval}{timeframe}" for interval in interval_list])}
        FROM {coin}usdt
        WHERE CONVERT(DATE, timestamp) = '{selected_date}'
        GROUP BY price
    """

    with connection.cursor(as_dict=True) as cursor:
        cursor.execute(query)
        data = cursor.fetchall()

    df = pd.DataFrame(data)
    df = df.apply(pd.to_numeric, errors='ignore')  # Convert all columns to numeric
    df = df.sort_values('price', ascending=False)
    # Get the intersection of existing DataFrame columns and expected volume_columns
    volume_columns = [col for col in df.columns if 'volume_' in col]

    if volume_columns:
        # Filter out rows where all volume columns are 0
        df = df.loc[~(df[volume_columns] == 0).all(axis=1)]

    # Rename the columns for better display
    df.columns = ['Price'] + column_names

    # Set the price column as the index
    df.set_index('Price', inplace=True)

    # Apply styling to lock the price column
    df_styled = df.style.set_table_styles([
        {'selector': 'th:first-child', 'props': [('position', 'sticky'), ('left', '0')]},
        {'selector': 'td:first-child', 'props': [('position', 'sticky'), ('left', '0')]},
        {'selector': 'td', 'props': [('text-align', 'right')]},
    ])

    return df_styled


def main():
    # Set Streamlit app title and layout
    st.title("Cryptocurrency Market Trading Data")
    st.write("Market data retrieved from SQL Server")

    # Get the list of coins
    coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil", "cfx", "adx", "chz", "bel", "alpaca", "elf", "epx", "pros", "t", "dar", "agix", "mob", "id", "trx", "key", "tru", "amb", "magic", "lina", "lever"]


    # Create a selectbox for coin selection
    selected_coin = st.selectbox("Select a coin", coins)

    # Fetch and display trading data for the selected coin
    df_trading = fetch_trading_data(selected_coin)
    st.subheader("Latest Trading Data")
    st.write(df_trading)


    selected_date = st.date_input('Select a date', datetime.now())
    selected_date = pd.to_datetime(selected_date).strftime('%Y-%m-%d')


    # Add a selection for timeframes
    timeframes = ["5min", "15min", "1hour"]
    selected_timeframe = st.selectbox("Select a timeframe", timeframes)

    # Fetch and display daily data for the selected coin and date
    df_daily = fetch_daily_data(selected_coin, selected_date, selected_timeframe)
    st.subheader("Daily Chart Data")
    st.write(df_daily)

    current_time = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
    st.write(f"Current Time: {current_time}")

    time.sleep(20)
    st.experimental_rerun()

if __name__ == '__main__':
    main()
