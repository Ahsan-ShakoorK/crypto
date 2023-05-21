import streamlit as st
import psycopg2
import pandas as pd

# Establish connection to PostgreSQL server
def get_db_connection():
    connection = psycopg2.connect(
        host='ahsanpost.postgres.database.azure.com',
        user='ahsan',
        password='name@123',
        dbname='postdb',
        sslmode='require'
    )
    return connection

# Function to fetch trading data from PostgreSQL for a specific coin
def fetch_trading_data(coin):
    query = f"""
        SELECT price,
            SUM(CASE WHEN timestamp >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '5 minutes' AND timestamp < DATE_TRUNC('second', CURRENT_TIMESTAMP) THEN volume ELSE 0 END) AS volume_5min,
            SUM(CASE WHEN timestamp >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '10 minutes' AND timestamp < DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '5 minutes' THEN volume ELSE 0 END) AS volume_5min_before,
            SUM(CASE WHEN timestamp >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '15 minutes' AND timestamp < DATE_TRUNC('second', CURRENT_TIMESTAMP) THEN volume ELSE 0 END) AS volume_15min,
            SUM(CASE WHEN timestamp >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '30 minutes' AND timestamp < DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '15 minutes' THEN volume ELSE 0 END) AS volume_15min_before,
            SUM(CASE WHEN timestamp >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '60 minutes' AND timestamp < DATE_TRUNC('second', CURRENT_TIMESTAMP) THEN volume ELSE 0 END) AS volume_60min,
            SUM(CASE WHEN timestamp >= DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '120 minutes' AND timestamp < DATE_TRUNC('second', CURRENT_TIMESTAMP) - INTERVAL '60 minutes' THEN volume ELSE 0 END) AS volume_60min_before
        FROM {coin}usdt
        WHERE timestamp >= CURRENT_DATE
        GROUP BY price
    """
    df = pd.read_sql_query(query, get_db_connection())
    df = df.rename(columns={
        'volume_5min': '5m',
        'volume_5min_before': '5m_b',
        'volume_15min': '15m',
        'volume_15min_before': '15m_b',
        'volume_60min': '60m',
        'volume_60min_before': '60m_b'})
    # Round the numbers to 8 decimal places
    df = df.round(8)
    df = st.write(df)
    return df

def main():
    # Set Streamlit app title and layout
    # st.title("Cryptocurrency Market Trading Data")
    # st.write("Market data retrieved from PostgreSQL server")

    # Get the list of coins
    coins = ["sxp", "chess", "blz", "joe", "perl"]

    # Create a selectbox for coin selection
    selected_coin = st.selectbox("Select a coin", coins)

    # Fetch trading data for the selected coin
    df = fetch_trading_data(selected_coin)

    # Inject custom CSS to style the table
   # Apply CSS styling to the table
    table_style = """
        <style>
        .dataframe {
            width: 90%;
            margin: auto;
        }
        </style>
    """
    st.markdown(table_style, unsafe_allow_html=True)




if __name__ == '__main__':
    main()
