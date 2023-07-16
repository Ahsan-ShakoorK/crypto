import streamlit as st
import pandas as pd
import pymongo
from datetime import datetime, timedelta
import time
from io import BytesIO
import numpy as np
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime, timedelta
import pytz

# Modify pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)
pd.set_option('display.precision', 10)

uri = "mongodb+srv://randb:randb@rancluster.wt5dwr8.mongodb.net/?retryWrites=true&w=majorit"
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client['db_ran']  # Name of your database
def round_time(dt=None, round_to=15):
   if dt == None : dt = datetime.now()
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   rounding = (seconds+round_to/2) // round_to * round_to
   return dt + timedelta(0,rounding-seconds,-dt.microsecond)

def fetch_trading_data(coin):
    collection = db[f'{coin}_trades']
    now = datetime.now().replace(second=0, microsecond=0) 
    now = now.astimezone(pytz.utc)  # Convert the time to UTC

    # Calculate the timeframes
    five_minute = now - timedelta(minutes=now.minute % 5)
# Calculate the timeframes
    now = datetime.now().replace(second=0, microsecond=0) 
    now = now.astimezone(pytz.utc)  # Convert the time to UTC

# Round current time to the nearest 15 minute mark
    fifteen_minute = round_time(now, 15*60)

#    Now, fifteen_minute should be on a 15 minute interval

    fifteen_minute_before = now - timedelta(minutes=now.minute % 15 + 15)
    one_hour = now.replace(minute=0)

    # Define a function to generate the common query structure
    def generate_query(start_time, end_time, time_label):
        return [
            {
                '$match': {
                    'timestamp': {
                        '$gte': start_time,
                        '$lt': end_time
                    }
                }
            },
            {
                '$group': {
                    '_id': '$price',
                    f'quantity_{time_label}': {'$sum': '$quantity'}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'price': '$_id',
                    f'quantity_{time_label}': 1
                }
            }
        ]

    # Fetch the data for each time period
    df_all = pd.DataFrame()
    for i, time_interval in enumerate([(five_minute, "5min"), (five_minute - timedelta(minutes=5), "5min_before"), 
                                       (fifteen_minute, "15min"), (fifteen_minute_before, "15min_before"), 
                                       (one_hour, "60min"), (one_hour - timedelta(hours=1), "60min_before")]):

        start_time, label = time_interval
        end_time = start_time + (timedelta(minutes=5) if "5min" in label else timedelta(minutes=15) if "15min" in label else timedelta(hours=1))
        query = generate_query(start_time, end_time, label)

        result = collection.aggregate(query)
        data = list(result)
        df = pd.DataFrame(data)

        if 'price' not in df.columns:
            df['price'] = np.nan
            df[f'quantity_{label}'] = 0

        if df.empty:
            df = pd.DataFrame([{'price': 0, f'quantity_{label}': 0}])

        df = df[df['price'] != 0]

        print(f"Iteration {i} columns: {df.columns}")

        if i == 0:
            df_all = df
        else:
            df_all = df_all.merge(df, on='price', how='outer')

    columns = ['price'] + [col for col in df_all.columns if col != 'price']
    df_all = df_all[columns]

    df_all['price'] = df_all['price'].apply(lambda x: '{:.10f}'.format(x))
    df_all = df_all.fillna(0)

    quantity_columns = ['quantity_5min', 'quantity_5min_before', 'quantity_15min', 'quantity_15min_before',
                      'quantity_60min', 'quantity_60min_before']

    column_mapping = {
        'quantity_5min': '5m',
        'quantity_5min_before': '5m_b',
        'quantity_15min': '15m',
        'quantity_15min_before': '15m_b',
        'quantity_60min': '60m',
        'quantity_60min_before': '60m_b'
    }
    df_all = df_all.rename(columns=column_mapping)

    df_styled = df_all.style.set_table_styles([
        {'selector': 'th:first-child', 'props': [('position', 'sticky'), ('left', '0')]},
        {'selector': 'td:first-child', 'props': [('position', 'sticky'), ('left', '0')]},
        {'selector': 'td', 'props': [('text-align', 'right')]}
    ])

    return df_styled




def highlight_greater_values(x, value):
    if isinstance(x, (int, float)) and x > value:
        return 'background-color: yellow'
    return ''


from dateutil.relativedelta import relativedelta

def fetch_daily_data_combined(coin, selected_date, timeframe, value=None, highlight=False, percentage=False):
    collection = db[f'{coin}_trades']
    start_time = pd.to_datetime(selected_date)
    end_time = start_time + timedelta(days=1)
    if timeframe == "1hour":
        time_interval = 60
    else:
        time_interval = int(timeframe[:-3])


    pipeline = [
        {
            '$match': {
                'timestamp': {'$gte': start_time, '$lt': end_time}
            }
        },
        {
            '$addFields': {
                'time_bucket': {
                    '$subtract': [
                        {'$toLong': '$timestamp'},
                        {'$mod': [{'$toLong': '$timestamp'}, 1000 * 60 * time_interval]}
                    ]
                }
            }
        },
        {
            '$group': {
                '_id': {'price': '$price', 'time_bucket': '$time_bucket'},
                'quantity': {'$sum': '$quantity'}
            }
        },
        {
            '$project': {
                '_id': 0,
                'price': '$_id.price',
                'quantity': 1,
                'timestamp': '$_id.time_bucket'
            }
        }
    ]

   

    result = collection.aggregate(pipeline, allowDiskUse=True)
    df = pd.DataFrame(list(result))
    df.set_index('price', inplace=True)

    # Reset index, apply formatting, and set index again
    df = df.reset_index()
    df['price'] = df['price'].apply(lambda x: '{:.11f}'.format(x))
    df.set_index('price', inplace=True)   
    # Filter out rows where index (price) is 0
    df = df[df.index != 0]

    df_pivot = df.pivot(columns='timestamp', values='quantity')
    df_pivot.columns = pd.to_datetime(df_pivot.columns, unit='ms').strftime('%H:%M')



    if highlight:
        df_pivot = df_pivot.style.applymap(highlight_greater_values, value=value)

    if percentage:
        df_pivot = df_pivot.apply(lambda x: (x / value) * 100 if pd.notnull(x) else x)

    return df_pivot


def to_excel_bytes(df):
    if isinstance(df, pd.io.formats.style.Styler):
        df = df.data  # access the original DataFrame for Styler object
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Sheet1')
    output.seek(0)
    return output.getvalue()
pd.set_option('display.float_format', '{:.10f}'.format)

def main():
    try:
        # Set Streamlit app title and layout
        # st.title("Cryptocurrency Market Trading Data")
        # st.write("Market data retrieved from MongoDB")
        # Display the current time
        current_time = pd.to_datetime('now', utc=True).strftime("%Y-%m-%d %H:%M:%S")
        st.write(f"Current Time: {current_time}")

        # Get the list of coins
        coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil", "cfx", "adx", "chz", "bel",
                 "alpaca", "elf", "epx", "pros", "t", "dar", "agix", "mob", "id", "trx", "key", "tru", "amb", "magic",
                 "lina", "lever", "btc", "eth", "tomo", "dodo", "cvp", "data", "ata", "cos", "fida", "fis", "loom",
                 "super", "pepe", "matic", "ada", "doge", "mav", "xec", "sui", "eos", "ftm", "xlm"]

        # Create a selectbox for coin selection
        selected_coin = st.selectbox("Select a coin", coins)

        # Fetch and display trading data for the selected coin
        df_trading = fetch_trading_data(selected_coin)
        st.write(df_trading.data)

        selected_date = st.date_input('Select a date', datetime.now())
        selected_date = pd.to_datetime(selected_date).strftime('%Y-%m-%d')

        # Add a selection for timeframes
        timeframes = ["5min", "15min", "1hour"]
        selected_timeframe = st.selectbox("Timeframe", timeframes)

        df_daily_styled = fetch_daily_data_combined(selected_coin, selected_date, selected_timeframe)

        # Highlight values greater than a certain threshold
        highlight_enabled = st.checkbox("Highlight values greater than:")
        if highlight_enabled:
            highlight_value = st.number_input("Enter the value for highlighting", min_value=0)
            df_daily_styled = fetch_daily_data_combined(selected_coin, selected_date, selected_timeframe,
                                                        value=highlight_value, highlight=True)

        # Display daily data in percentage
        percentage_enabled = st.checkbox("Display data in percentage")
        if percentage_enabled:
            percentage_value = st.number_input("Enter the value for percentage calculation", min_value=0)
            df_daily_styled = fetch_daily_data_combined(selected_coin, selected_date, selected_timeframe,
                                                        value=percentage_value, percentage=True)

        st.subheader("Daily Chart Data")
        with pd.option_context('display.float_format', '{:0.10f}'.format):
            st.write(df_daily_styled)

        # Download button for Excel
        if st.button("Download Daily Chart Data as Excel"):
            excel_bytes = to_excel_bytes(df_daily_styled)
            st.download_button(
                label="Click to Download",
                data=excel_bytes,
                file_name=f"daily_chart_data_{selected_coin}_{selected_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        # Display the current time
        current_time = pd.to_datetime('now', utc=True).strftime("%Y-%m-%d %H:%M:%S")
        st.write(f"Current Time: {current_time}")

        # Rerun the app every 30 seconds
        time.sleep(30)
        st.experimental_rerun()

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")


if __name__ == '__main__':
    main()
