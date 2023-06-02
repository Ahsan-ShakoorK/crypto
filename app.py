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

def fetch_trading_data(coin):
    query_5min = f"""
        SELECT ROUND(price, 6) AS price,
            SUM(volume) AS total_volume
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

    df_5min = pd.DataFrame(data_5min)
    df_hourly = pd.DataFrame(data_hourly)

    st.subheader("5-Minute Trading Data")
    st.write(df_5min)

    st.subheader("Hourly Trading Data")
    selected_date = st.selectbox("Select a date", df_hourly['hour'].unique())
    filtered_data = df_hourly[df_hourly['hour'] == selected_date]
    filtered_data = filtered_data.explode('prices').reset_index(drop=True)
    filtered_data['prices'] = filtered_data['prices'].str.replace('[\[\]]', '')
    st.write(filtered_data)


def main():
    # Set Streamlit app title and layout
    # st.title("Cryptocurrency Market Trading Data")
    # st.write("Market data retrieved from MySQL server")

    # Get the list of coins
    coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil"]

    # Create a selectbox for coin selection
    selected_coin = st.selectbox("Select a coin", coins)

    # Fetch trading data for the selected coin
    fetch_trading_data(selected_coin)
    time.sleep(5)
    st.experimental_rerun()


if __name__ == '__main__':
    main()
