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

# Function to fetch daily trading data with hourly time frames for a specific coin
def fetch_daily_trading_data(coin):
    query = f"""
        SELECT DATE(timestamp) AS date,
            HOUR(timestamp) AS hour,
            SUM(volume) AS total_volume
        FROM {coin}usdt
        WHERE timestamp >= CURDATE() - INTERVAL 7 DAY
        GROUP BY date, hour
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    
    df = pd.DataFrame(data)
    df = df.pivot(index='hour', columns='date', values='total_volume')
    df = df.rename_axis(None, axis=1)  # Remove axis labels for cleaner table
    st.write(df)


def main():
    # Set Streamlit app title and layout
    # st.title("Cryptocurrency Market Trading Data")
    # st.write("Market data retrieved from MySQL server")

    # Get the list of coins
    coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil"]

    # Create a selectbox for coin selection
    selected_coin = st.selectbox("Select a coin", coins)

    # Fetch and display daily trading data for the selected coin
    fetch_daily_trading_data(selected_coin)
    
    time.sleep(5)
    st.experimental_rerun()


if __name__ == '__main__':
    main()
