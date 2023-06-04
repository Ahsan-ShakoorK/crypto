import streamlit as st
from sqlalchemy import create_engine
import pandas as pd

def get_db_connection():
    engine = create_engine('mssql+pyodbc://dbahsantrade:Pak@1947@A2NWPLSK14SQL-v06.shr.prod.iad2.secureserver.net/db_ran?driver=ODBC+Driver+17+for+SQL+Server')
    connection = engine.connect()
    return connection

def fetch_data(coin):
    query = f"""
        SELECT 
            ROUND(price, 6) AS price,
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/300)*300, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/300)*300 + 300, '19700101') THEN volume ELSE 0 END) AS volume_5min,
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/300)*300 - 300, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/300)*300, '19700101') THEN volume ELSE 0 END) AS volume_5min_before,
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/900)*900, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/900)*900 + 900, '19700101') THEN volume ELSE 0 END) AS volume_15min,
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/900)*900 - 900, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/900)*900, '19700101') THEN volume ELSE 0 END) AS volume_15min_before,
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/3600)*3600, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/3600)*3600 + 3600, '19700101') THEN volume ELSE 0 END) AS volume_60min,
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/3600)*3600 - 3600, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/3600)*3600, '19700101') THEN volume ELSE 0 END) AS volume_60min_before
        FROM {coin}usdt
        WHERE timestamp >= GETDATE()
        GROUP BY price
    """
    connection = get_db_connection()
    df = pd.read_sql_query(query, connection)
    connection.close()

    return df

def main():
    # Set Streamlit app title and layout
    st.title("Cryptocurrency Market Data")
    st.write("Market data retrieved from SQL Server")

    # Get the list of coins
    coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil", "cfx", "adx", "chz", "bel", "alpaca", "elf", "epx", "pros", "t", "dar", "agix", "mob", "id", "trx", "key", "tru", "amb", "magic", "lina", "lever"]

    # Create a selectbox for coin selection
    selected_coin = st.selectbox("Select a coin", coins)

    # Fetch trading data for the selected coin
    df = fetch_data(selected_coin)

    # Display the data
    st.dataframe(df)

if __name__ == '__main__':
    main()
