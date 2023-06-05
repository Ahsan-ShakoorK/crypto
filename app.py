import streamlit as st
import pymssql
import pandas as pd
import datetime
import time

# Establish connection to SQL Server
connection = pymssql.connect(
    server='A2NWPLSK14SQL-v06.shr.prod.iad2.secureserver.net',
    user='dbahsantrade',
    password='Pak@1947',
    database='db_ran'
)

def fetch_trading_data(coin):
    current_time = datetime.datetime.now()
    query = f"""
        SELECT ROUND(price, 6) AS price,
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/300)*300, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/300)*300 + 300, '19700101') THEN volume ELSE 0 END) AS "volume_{current_time.strftime('%H:%M:%S')}",
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/300)*300 - 300, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/300)*300, '19700101') THEN volume ELSE 0 END) AS "volume_{(current_time - datetime.timedelta(minutes=5)).strftime('%H:%M:%S')}",
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/900)*900, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/900)*900 + 900, '19700101') THEN volume ELSE 0 END) AS "volume_{current_time.strftime('%H:%M:%S')}",
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/900)*900 - 900, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/900)*900, '19700101') THEN volume ELSE 0 END) AS "volume_{(current_time - datetime.timedelta(minutes=15)).strftime('%H:%M:%S')}",
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/3600)*3600, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/3600)*3600 + 3600, '19700101') THEN volume ELSE 0 END) AS "volume_{current_time.strftime('%H:%M:%S')}",
            SUM(CASE WHEN timestamp >= DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/3600)*3600 - 3600, '19700101') AND timestamp < DATEADD(SECOND, FLOOR(DATEDIFF(SECOND, '19700101', GETDATE())/3600)*3600, '19700101') THEN volume ELSE 0 END) AS "volume_{(current_time - datetime.timedelta(hours=1)).strftime('%H:%M:%S')}"
        FROM {coin}usdt
        WHERE timestamp >= CAST(GETDATE() AS DATE)
        GROUP BY price
    """

    with connection.cursor(as_dict=True) as cursor:
        cursor.execute(query)
        data = cursor.fetchall()

    # Filter out rows where all volume columns are 0
    df = pd.DataFrame(data)
    volume_columns = [f"volume_{current_time.strftime('%H:%M:%S')}", f"volume_{(current_time - datetime.timedelta(minutes=5)).strftime('%H:%M:%S')}", f"volume_{current_time.strftime('%H:%M:%S')}", f"volume_{(current_time - datetime.timedelta(minutes=15)).strftime('%H:%M:%S')}", f"volume_{current_time.strftime('%H:%M:%S')}", f"volume_{(current_time - datetime.timedelta(hours=1)).strftime('%H:%M:%S')}"]
    df = df.loc[~(df[volume_columns] == 0).all(axis=1)]

    return df

def main():
    # Set Streamlit app title and layout
    st.title("Cryptocurrency Market Trading Data")
    st.write("Market data retrieved from SQL Server")

    # Get the list of coins
    coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil"]

    # Create a selectbox for coin selection
    selected_coin = st.selectbox("Select a coin", coins)

    # Fetch trading data for the selected coin
    df = fetch_trading_data(selected_coin)
    st.write(df)  # Display the DataFrame
    st.write('Current time: ', datetime.datetime.now().strftime('%H:%M:%S')) # Display the current time
    time.sleep(5)
    st.experimental_rerun()

if __name__ == '__main__':
    main()
