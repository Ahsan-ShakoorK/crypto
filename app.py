import streamlit as st
import pymysql.cursors
import pandas as pd

# Establish connection to MySQL server
connection = pymysql.connect(
    host='twenty.mysql.database.azure.com',
    user='ahsan',
    password='name@123',
    db='named',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
    ssl={'ca': 'DigiCertGlobalRootCA.crt.pem'}
)

# Function to fetch trading data from MySQL for a specific coin
def fetch_trading_data(coin):
    query = f"""
        SELECT price,
               SUM(CASE WHEN timestamp >= NOW() - INTERVAL 5 MINUTE THEN volume ELSE 0 END) AS volume_5min,
               SUM(CASE WHEN timestamp >= NOW() - INTERVAL 5 MINUTE - INTERVAL 5 MINUTE THEN volume ELSE 0 END) AS volume_5min_before,
               SUM(CASE WHEN timestamp >= NOW() - INTERVAL 15 MINUTE THEN volume ELSE 0 END) AS volume_15min,
               SUM(CASE WHEN timestamp >= NOW() - INTERVAL 15 MINUTE - INTERVAL 15 MINUTE THEN volume ELSE 0 END) AS volume_15min_before,
               SUM(CASE WHEN timestamp >= NOW() - INTERVAL 60 MINUTE THEN volume ELSE 0 END) AS volume_60min,
               SUM(CASE WHEN timestamp >= NOW() - INTERVAL 60 MINUTE - INTERVAL 60 MINUTE THEN volume ELSE 0 END) AS volume_60min_before
        FROM {coin}usdt
        WHERE timestamp >= CURDATE() + INTERVAL 1 SECOND
        GROUP BY price
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    
    # Filter out rows where all volume columns are 0
    df = pd.DataFrame(data)
    volume_columns = ['volume_5min', 'volume_5min_before', 'volume_15min', 'volume_15min_before', 'volume_60min', 'volume_60min_before']
    df = df[~(df[volume_columns] == 0).all(axis=1)]
    
    return df


def main():
    # Set Streamlit app title and layout
    st.title("Cryptocurrency Market Trading Data")
    st.write("Market data retrieved from MySQL server")

    # Get the list of coins
    coins = ["sxp", "chess", "blz", "joe", "perl"]

    # Create a selectbox for coin selection
    selected_coin = st.selectbox("Select a coin", coins)

    # Fetch trading data for the selected coin
    df = fetch_trading_data(selected_coin)

    # Display the table
    st.table(df)

if __name__ == '__main__':
    main()
