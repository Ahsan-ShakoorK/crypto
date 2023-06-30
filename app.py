import streamlit as st
import pymssql
import pandas as pd
import time
from datetime import datetime, timedelta
import io
from io import BytesIO


# Establish connection to SQL Server
connection = pymssql.connect(
    server='A2NWPLSK14SQL-v06.shr.prod.iad2.secureserver.net',
    user='dbahsantrade',
    password='Pak@1947',
    database='db_ran'
)

def fetch_trading_data(coin):
    pd.set_option('display.float_format', '{:.8g}'.format)
    now = datetime.now()
    five_minute = now - timedelta(minutes=now.minute % 5, seconds=now.second, microseconds=now.microsecond)
    fifteen_minute = now - timedelta(minutes=now.minute % 15, seconds=now.second, microseconds=now.microsecond)
    one_hour = now.replace(minute=0, second=0, microsecond=0)

    query = f"""
        SELECT price,
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


    df = pd.DataFrame(data)
    df = df.apply(pd.to_numeric, errors='ignore')  # Convert all columns to numeric
    df = df.sort_values('price', ascending=False)

    volume_columns = ['volume_5min', 'volume_5min_before', 'volume_15min', 'volume_15min_before', 'volume_60min', 'volume_60min_before']

    # Apply rounding to the volume columns
    df[volume_columns] = df[volume_columns].round(0)

    df = df.loc[~(df[volume_columns] == 0).all(axis=1)]
    df = df.rename(columns={
        'volume_5min': '5m',
        'volume_5min_before': '5m_b',
        'volume_15min': '15m',
        'volume_15min_before': '15m_b',
        'volume_60min': '60m',
        'volume_60min_before': '60m_b'
    })
    df.columns = ['price'] + volume_columns
    df['price'] = df['price'].apply(lambda x: f"{x:.8f}")
    df.set_index('price', inplace=True)

    df_styled = df.style.set_table_styles([
        {'selector': 'th:first-child', 'props': [('position', 'sticky'), ('left', '0')]},
        {'selector': 'td:first-child', 'props': [('position', 'sticky'), ('left', '0')]},
        {'selector': 'td', 'props': [('text-align', 'right')]}
    ])

    return df_styled


def highlight_greater_values(x, value):
    if isinstance(x, (int, float)) and x > value:
        return 'background-color: yellow'
    return ''

def fetch_daily_data_combined(coin, selected_date, timeframe, value=None, mode='highlight'):
    intervals = {
        '5min': list(range(0, 24 * 60, 5)),
        '15min': list(range(0, 24 * 60, 15)),
        '1hour': list(range(24))
    }
    interval_list = intervals[timeframe]

    if timeframe == '5min':
        column_names = [f"{str(interval // 60).zfill(2)}:{str(interval % 60).zfill(2)}" for interval in interval_list]
        interval_aggregations = [
            f"SUM(CASE WHEN DATEPART(HOUR, timestamp) = {interval // 60} AND DATEPART(MINUTE, timestamp) >= {interval % 60} AND DATEPART(MINUTE, timestamp) < {interval % 60 + 5} THEN volume ELSE 0 END) AS volume_{interval}_5min"
            for interval in interval_list]
    elif timeframe == '15min':
        column_names = [f"{str(interval // 60).zfill(2)}:{str(interval % 60).zfill(2)}" for interval in interval_list]
        interval_aggregations = [
            f"SUM(CASE WHEN DATEPART(HOUR, timestamp) = {interval // 60} AND DATEPART(MINUTE, timestamp) >= {interval % 60} AND DATEPART(MINUTE, timestamp) < {interval % 60 + 15} THEN volume ELSE 0 END) AS volume_{interval}_15min"
            for interval in interval_list]
    else:
        column_names = [f"{str(interval).zfill(2)}:00" for interval in interval_list]
        interval_aggregations = [
            f"SUM(CASE WHEN DATEPART(HOUR, timestamp) = {interval} THEN volume ELSE 0 END) AS volume_{interval}_hour"
            for interval in interval_list]

    query = f"""
        SELECT price,
            {', '.join(interval_aggregations)}
        FROM {coin}usdt
        WHERE CONVERT(DATE, timestamp) = '{selected_date}'
        GROUP BY price
    """


    with connection.cursor(as_dict=True) as cursor:
        cursor.execute(query)
        data = cursor.fetchall()

    df = pd.DataFrame(data)
    df = df.apply(pd.to_numeric, errors='ignore')
    df = df.sort_values('price', ascending=False)
    volume_columns = [col for col in df.columns if 'volume_' in col]

    if volume_columns:
        df = df.loc[~(df[volume_columns] == 0).all(axis=1)]

    df.columns = ['price'] + column_names
    df['price'] = df['price'].apply(lambda x: f"{x:.8f}")
    df.set_index('price', inplace=True)

    df_styled = df.style.set_table_styles([
        {'selector': 'th:first-child', 'props': [('position', 'sticky'), ('left', '0')]},
        {'selector': 'td:first-child', 'props': [('position', 'sticky'), ('left', '0')]},
        {'selector': 'td', 'props': [('text-align', 'right')]},
    ])

    if mode == 'highlight':
        if value is not None:
            df_styled = df_styled.applymap(lambda x: highlight_greater_values(x, value), subset=column_names)
    elif mode == 'percentage':
        if value is not None:
            df_styled.data = df.apply(lambda x: (x / value) * 100 if value else x)
            df_styled = df_styled.applymap(lambda x: 'background-color: yellow' if x > 100 else '', subset=column_names)  # Highlight values > 100
    else:
        raise ValueError("Invalid mode. Choose either 'highlight' or 'percentage'.")
    
    return df_styled



def to_excel_bytes(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Sheet1')
    output.seek(0)
    return output.getvalue()

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
    selected_timeframe = st.selectbox("Timeframe", timeframes)

    # Fetch daily data
    df_daily_styled = fetch_daily_data_combined(selected_coin, selected_date, selected_timeframe, mode='highlight')

    # Highlight values greater than a certain threshold
    highlight_enabled = st.checkbox("Highlight values greater than:")
    if highlight_enabled:
        highlight_value = st.number_input("Enter the value for highlighting", min_value=0)
        df_daily_styled = fetch_daily_data_combined(selected_coin, selected_date, selected_timeframe, value=highlight_value, mode='highlight')

    # Display daily data in percentage
    percentage_enabled = st.checkbox("Display data in percentage")
    if percentage_enabled:
        percentage_value = st.number_input("Enter the value for percentage calculation", min_value=0)
        df_daily_styled = fetch_daily_data_combined(selected_coin, selected_date, selected_timeframe, value=percentage_value, mode='percentage')

    st.subheader("Daily Chart Data")
    st.write(df_daily_styled.data)

    # Download button for Excel
    if st.button("Download Daily Chart Data as Excel"):
        excel_bytes = to_excel_bytes(df_daily_styled.data)
        st.download_button(
            label="Click to Download",
            data=excel_bytes,
            file_name=f"daily_chart_data_{selected_coin}_{selected_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    # Display the current time
    current_time = pd.to_datetime('now').strftime("%Y-%m-%d %H:%M:%S")
    st.write(f"Current Time: {current_time}")

    # Rerun the app every 30 seconds
    time.sleep(30)
    st.experimental_rerun()


if __name__ == '__main__':
    main()
