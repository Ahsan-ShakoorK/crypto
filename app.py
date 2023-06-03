import streamlit as st
import pymysql.cursors
import pandas as pd
import time

# Establish connection to MySQL server
connection = pymysql.connect(
    host='usa.mysql.database.azure.com',
    user='ahsan',
    password='name@123',
    db='sqldb',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
    ssl={'ca': 'DigiCertGlobalRootCA.crt.pem'}
)

# Function to fetch trading data from MySQL for a specific coin
def fetch_trading_data(coin):
    query_5min = f"""
        SELECT ROUND(price, 4) AS price,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/300)*300) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/300)*300 + 300) THEN volume ELSE 0 END) AS volume_5min,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/300)*300 - 300) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/300)*300) THEN volume ELSE 0 END) AS volume_5min_before,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/900)*900) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/900)*900 + 900) THEN volume ELSE 0 END) AS volume_15min,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/900)*900 - 900) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/900)*900) THEN volume ELSE 0 END) AS volume_15min_before,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/3600)*3600) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/3600)*3600 + 3600) THEN volume ELSE 0 END) AS volume_60min,
            SUM(CASE WHEN timestamp >= FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/3600)*3600 - 3600) AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW())/3600)*3600) THEN volume ELSE 0 END) AS volume_60min_before
        FROM {coin}usdt
        WHERE timestamp >= CURDATE() + INTERVAL 1 SECOND
        GROUP BY price
    """

    query_daily = f"""
        SELECT price,
            SUM(CASE WHEN HOUR(timestamp) = 0 THEN volume ELSE 0 END) AS hour_0,
            SUM(CASE WHEN HOUR(timestamp) = 1 THEN volume ELSE 0 END) AS hour_1,
            SUM(CASE WHEN HOUR(timestamp) = 2 THEN volume ELSE 0 END) AS hour_2,
            SUM(CASE WHEN HOUR(timestamp) = 3 THEN volume ELSE 0 END) AS hour_3,
            SUM(CASE WHEN HOUR(timestamp) = 4 THEN volume ELSE 0 END) AS hour_4,
            SUM(CASE WHEN HOUR(timestamp) = 5 THEN volume ELSE 0 END) AS hour_5,
            SUM(CASE WHEN HOUR(timestamp) = 6 THEN volume ELSE 0 END) AS hour_6,
            SUM(CASE WHEN HOUR(timestamp) = 7 THEN volume ELSE 0 END) AS hour_7,
            SUM(CASE WHEN HOUR(timestamp) = 8 THEN volume ELSE 0 END) AS hour_8,
            SUM(CASE WHEN HOUR(timestamp) = 9 THEN volume ELSE 0 END) AS hour_9,
            SUM(CASE WHEN HOUR(timestamp) = 10 THEN volume ELSE 0 END) AS hour_10,
            SUM(CASE WHEN HOUR(timestamp) = 11 THEN volume ELSE 0 END) AS hour_11,
            SUM(CASE WHEN HOUR(timestamp) = 12 THEN volume ELSE 0 END) AS hour_12,
            SUM(CASE WHEN HOUR(timestamp) = 13 THEN volume ELSE 0 END) AS hour_13,
            SUM(CASE WHEN HOUR(timestamp) = 14 THEN volume ELSE 0 END) AS hour_14,
            SUM(CASE WHEN HOUR(timestamp) = 15 THEN volume ELSE 0 END) AS hour_15,
            SUM(CASE WHEN HOUR(timestamp) = 16 THEN volume ELSE 0 END) AS hour_16,
            SUM(CASE WHEN HOUR(timestamp) = 17 THEN volume ELSE 0 END) AS hour_17,
            SUM(CASE WHEN HOUR(timestamp) = 18 THEN volume ELSE 0 END) AS hour_18,
            SUM(CASE WHEN HOUR(timestamp) = 19 THEN volume ELSE 0 END) AS hour_19,
            SUM(CASE WHEN HOUR(timestamp) = 20 THEN volume ELSE 0 END) AS hour_20,
            SUM(CASE WHEN HOUR(timestamp) = 21 THEN volume ELSE 0 END) AS hour_21,
            SUM(CASE WHEN HOUR(timestamp) = 22 THEN volume ELSE 0 END) AS hour_22,
            SUM(CASE WHEN HOUR(timestamp) = 23 THEN volume ELSE 0 END) AS hour_23
        FROM {coin}usdt
        WHERE timestamp >= CURDATE() - INTERVAL 7 DAY
        GROUP BY price
    """

    with connection.cursor() as cursor:
        cursor.execute(query_5min)
        data_5min = cursor.fetchall()

        cursor.execute(query_daily)
        data_daily = cursor.fetchall()

    # Filter out rows where all volume columns are 0
    df_5min = pd.DataFrame(data_5min)
    volume_columns = ['volume_5min', 'volume_5min_before', 'volume_15min', 'volume_15min_before', 'volume_60min', 'volume_60min_before']
    df_5min = df_5min[~(df_5min[volume_columns] == 0).all(axis=1)]
    df_5min = df_5min.rename(columns={
        'volume_5min': '5m',
        'volume_5min_before': '5m_b',
        'volume_15min': '15m',
        'volume_15min_before': '15m_b',
        'volume_60min': '60m',
        'volume_60min_before': '60m_b'
    })

    df_daily = pd.DataFrame(data_daily)

    # Display table
    st.subheader("Trading Data real time")
    st.write(df_5min)

    st.subheader("Daily Trading Data")
    selected_date = st.selectbox("Select a date", df_daily['date'].unique())
    filtered_data = df_daily[df_daily['date'] == selected_date]
    st.write(filtered_data)


def main():
    # Set Streamlit app title and layout
    # st.title("Cryptocurrency Market Trading Data")
    # st.write("Market data retrieved from MySQL server")

    # Get the list of coins
    coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil", "cfx", "adx", "chz", "bel", "alpaca", "elf", "epx", "pros", "t", "dar", "agix", "mob", "id", "trx", "key", "tru", "amb", "magic", "lina", "lever"]

    # Create a selectbox for coin selection
    selected_coin = st.selectbox("Select a coin", coins)

    # Fetch trading data for the selected coin
    fetch_trading_data(selected_coin)
    time.sleep(5)
    st.experimental_rerun()


if __name__ == '__main__':
    main()
