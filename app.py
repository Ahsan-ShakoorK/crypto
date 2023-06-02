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
        SELECT ROUND(price, 6) AS price,
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

    query_hourly = f"""
        SELECT HOUR(timestamp) AS hour,
            GROUP_CONCAT(DISTINCT ROUND(price, 6) ORDER BY price SEPARATOR ', ') AS prices,
            GROUP_CONCAT(volume) AS volumes
        FROM {coin}usdt
        WHERE timestamp >= CURDATE() - INTERVAL 7 DAY
        GROUP BY hour
        ORDER BY hour ASC
    """

    with connection.cursor() as cursor:
        cursor.execute(query_5min)
        data_5min = cursor.fetchall()

        cursor.execute(query_hourly)
        data_hourly = cursor.fetchall()

    # Filter out rows where all volume columns are 0
    df_5min = pd.DataFrame(data_5min)
    volume_columns = ['volume_5min', 'volume_5min_before', 'volume_15min', 'volume_15min_before', 'volume_60min', 'volume_60min_before']
    df_5min = df_5min[~(df_5min[volume_columns] == 0).all(axis=1)]
    df_5min = df_5min.reindex(columns=['price', 'volume_5min', 'volume_5min_before', 'volume_15min', 'volume_15min_before', 'volume_60min', 'volume_60min_before'])

    df_hourly = pd.DataFrame(data_hourly)
    df_hourly['prices'] = df_hourly['prices'].apply(lambda x: x.split(', '))
    df_hourly['volumes'] = df_hourly['volumes'].apply(lambda x: x.split(', '))
    df_hourly['volumes'] = df_hourly['volumes'].apply(lambda x: sum(map(float, x)))
    df_hourly = df_hourly.reindex(columns=['hour', 'prices', 'volumes'])

    return df_5min, df_hourly

# Main Streamlit app
def main():
    st.title("Crypto Trading Data")

    coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil"]
    
    df_5min, df_hourly = fetch_trading_data(coin)

    st.subheader("5-Minute Trading Data")
    st.write(df_5min)

    st.subheader("Hourly Trading Data")
    st.write(df_hourly)

if __name__ == '__main__':
    main()
