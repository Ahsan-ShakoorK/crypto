import streamlit as st
import pymysql.cursors
import pandas as pd

# Establish connection to MySQL server
connection = pymysql.connect(
    host='usa.mysql.database.azure.com',
    user='dbusa',
    password='ahsan@123',
    db='python_db',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
    ssl={'ca': 'DigiCertGlobalRootCA.crt.pem'}
)

# Function to fetch trading data from MySQL for a specific coin
def fetch_trading_data(coin):
    query = f"""
        SELECT price,
               SUM(CASE WHEN timestamp >= NOW() - INTERVAL 5 MINUTE THEN volume ELSE 0 END) AS volume_5min,
               SUM(CASE WHEN timestamp >= NOW() - INTERVAL 15 MINUTE THEN volume ELSE 0 END) AS volume_15min,
               SUM(CASE WHEN timestamp >= NOW() - INTERVAL 60 MINUTE THEN volume ELSE 0 END) AS volume_60min,
               SUM(volume) AS total_coins_traded
        FROM {coin}usdt
        WHERE timestamp >= NOW() - INTERVAL 1 HOUR
        GROUP BY price
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    return data

def main():
    # Set Streamlit app title and layout
    st.title("Cryptocurrency Market Trading Data")
    st.write("Market data retrieved from MySQL server")

    # Get the list of coins
    coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil"]

    # Create a selectbox for coin selection
    selected_coin = st.selectbox("Select a coin", coins)

    # Fetch trading data for the selected coin
    data = fetch_trading_data(selected_co
    # Convert data to pandas DataFrame
    df = pd.DataFrame(data)

    # Display the table
    st.table(df)

if __name__ == '__main__':
    main()
