import time
from binance.client import Client
from binance.streams import BinanceSocketManager
from multiprocessing import Process
import pymysql.cursors

# Connect to the database
db = pymysql.connect(
    host='usadb.mysql.database.azure.com',
    user='ahsandb',
    password='ahsan@123',
    db='pythond',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
    ssl={'ca': 'DigiCertGlobalRootCA.crt.pem'}
)

# Binance API initialization
api_key = 'vijG9ARQVjevGky2bkTgFACUMmRJBVxVqwtzDIONOToNIw4YmTMxMgyy4PEVUhFH'
api_secret = 'f9BHEpkASNW9S0nd7xe1Iq55ccd0bFrwI8LL0mA0UlIgODhubsX8noG6TnWicvmh'
client = Client(api_key, api_secret)

cursor = db.cursor()

# List of coins
coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil"]

# Create table for each coin if it does not exist
for coin in coins:
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {coin}usdt (
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            price DECIMAL(18, 8),
            volume DECIMAL(18, 8)
        )
    """)

def process_message(msg, coin):
    try:
        if msg['e'] == 'trade':
            price = msg['p']
            volume = msg['q']
            # Insert data into MySQL
            cursor.execute(f"INSERT INTO {coin}usdt (price, volume) VALUES (%s, %s)", (price, volume))
            db.commit()
            print(f"Inserted trade data for {coin.upper()}: Price {price}, Volume {volume}")
    except pymysql.Error as err:
        print(f"Error occurred: {err}")
        db.rollback()  # Revert the changes if an error occurred

def start_websocket(coin):
    bsm = BinanceSocketManager(client)
    bsm.start_trade_socket(f"{coin.upper()}USDT", lambda msg: process_message(msg, coin))
    bsm.start()

# Start a separate process for each coin
for coin in coins:
    Process(target=start_websocket, args=(coin,)).start()

# Keep the script running
while True:
    time.sleep(3)
