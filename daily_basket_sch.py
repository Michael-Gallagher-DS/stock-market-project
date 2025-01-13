print('\nTask started.')

#Import Libraries
import config
import pandas as pd
import ta
from datetime import datetime
from tickers import *
from sqlalchemy import create_engine
import mysql.connector
import numpy as np

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data import StockHistoricalDataClient

print('Task complete: Libraries imported.')






### Retrieve daily bar data.
# Get todays date
today = datetime.today()

# Create Stock Client
stock_client = StockHistoricalDataClient(config.API_KEY, config.SECRET_KEY)

# Set the symbols, time frame and dates
stocks = BASKET
timeframe = TimeFrame(1, TimeFrameUnit.Day)
start_date = datetime(2016, 1, 1)
end_date = today

request_params = StockBarsRequest(
                        symbol_or_symbols=stocks,
                        timeframe=timeframe,
                        start=start_date,
                        end=end_date
                 )

bars_daily = stock_client.get_stock_bars(request_params)

df_daily = bars_daily.df
df_daily.reset_index(inplace=True)

df_daily

print("Task complete: API call successfull. Beginning preprocesssing.")





# Convert timestamp column to a datetime object so we can extract the date.
df_daily['timestamp'] = pd.to_datetime(df_daily['timestamp'])

# Convert timestamps to EST time
df_daily['timestamp'] = df_daily['timestamp'].dt.tz_convert('America/New_York')

# Extract date from 'timestamp' column
df_daily['date'] = df_daily['timestamp'].dt.date

# Drop trade_count and vwap
df_daily.drop(columns=['trade_count', 'vwap'], inplace=True)

# Sort by symbol and date
df_daily.sort_values(['symbol', 'date'], inplace=True)






#### Calculate the percentage change grouped by symbol
df_daily['pct_change'] = df_daily.groupby('symbol')['close'].apply(
    lambda group: group.pct_change() * 100
).round(2).reset_index(level=0, drop=True)

# Shift the 'close' column to align with the next day's 'open' column, grouped by symbol
df_daily['prev_close'] = df_daily.groupby(['symbol'])['close'].shift(1)

# Calculate the gap percentage grouped by symbol and date
df_daily['pct_gap'] = df_daily.apply(
    lambda row: ((row['open'] - row['prev_close']) / row['prev_close']) * 100 if row['prev_close'] != 0 else 0,
    axis=1
).round(2)

#Calculate the percentage change from today's open to close
df_daily['pct_open_to_close'] = ((df_daily['close'] - df_daily['open']) / df_daily['open'] * 100).round(2)

# Drop the previous close colums
df_daily.drop(columns=['prev_close'], inplace=True)

print("Checking percent change columns...\n",df_daily.head())







### Calculate RVOL and ATR

# Calculate the 14-period average volume
df_daily['adv'] = df_daily.groupby('symbol')['volume'].apply(
    lambda x: x.rolling(window=14).mean().round(0)
).reset_index(level=0, drop=True)

# Calculate RVOL
df_daily['rvol'] = (df_daily['volume'] / df_daily['adv']).round(2)






### Calculate ATR and DTR

def atr(df):
    if len(df) < 14:
        return pd.Series([np.nan] * len(df))  # Return NaN if the group is too small for ATR calculation
    else:
        return ta.volatility.AverageTrueRange(
            high=df['high'], low=df['low'], close=df['close'], window=14, fillna=False
        ).average_true_range().round(2)

# Apply ATR calculation grouped by symbol for correct assignment
df_daily['atr'] = df_daily.groupby('symbol').apply(
    lambda group: atr(group).reset_index(drop=True)
).values




# Replace 0s with NaN in the ATR column to avoid division by zero
df_daily['atr'].replace(0, np.nan, inplace=True)

# Calculate the DTR and add the column to the df
df_daily['dtr'] = (abs(df_daily['high'] - df_daily['low'])).round(2)

# Calculate a true range comparison - value > 1 = an outsized move
df_daily['tr_comp'] = (df_daily['dtr'] / df_daily['atr']).round(2)

print("\n\nChecking ATR calculation. ATR column should be blank for the first 14 bars\n", df_daily[df_daily['symbol'] == 'NVDA'].head(15),
"\n\n\nATR column should be complete but check for outliers\n", df_daily[df_daily['symbol'] == 'NVDA'].tail())









### Calculate moving averages and RSI

# Calculate all moving averages grouped by symbol (remove date from grouping)
def calculate_indicators(group):
    group['ema_9'] = ta.trend.EMAIndicator(close=group['close'], window=9, fillna=False).ema_indicator().round(2)
    group['ema_21'] = ta.trend.EMAIndicator(close=group['close'], window=21, fillna=False).ema_indicator().round(2)
    group['sma_50'] = ta.trend.SMAIndicator(close=group['close'], window=50, fillna=False).sma_indicator().round(2)
    group['sma_200'] = ta.trend.SMAIndicator(close=group['close'], window=200, fillna=False).sma_indicator().round(2)
    group['rsi'] = ta.momentum.RSIIndicator(close=group['close'], window=14, fillna=False).rsi().round(2)
    return group

# Apply the indicator calculations grouped by symbol
df_daily = df_daily.groupby('symbol').apply(calculate_indicators).reset_index(drop=True)

print("\n\nChecking MA's...\n", df_daily[df_daily['symbol'] == 'TSLA'].head(22))









### Trend features

# Create 'range_pct' column which is the percent of the closing price relative to the same day's trading range. Higher value means the stock closed strong, lower val indicates a weaker close.
df_daily['range_pct'] = round(((df_daily['close'] - df_daily['low']) / (df_daily['high'] - df_daily['low'])), 2)

# Create 'trend_strength' column as a calculation that returns a number between 1 and 0. "1" being a strong trend day in either direction, "0" being no trend at all. i.e., a candle opening far from LOD  and HOD and also closes far from LOD or HOD.
df_daily['trend_strength'] = round((abs(df_daily['open'] - df_daily['close']) / abs(df_daily['high'] - df_daily['low'])),2)

# Reorder columns
df_daily = df_daily[[
    'symbol', 'date', 'pct_change', 'pct_open_to_close', 'pct_gap',
    'open', 'high', 'low', 'close',
    'volume', 'adv','rvol',
    'atr', 'dtr', 'tr_comp',
    'ema_9', 'ema_21', 'sma_50','sma_200', 'rsi',
    'range_pct', 'trend_strength'
]]

print("Task complete: Preprocessing complete. Beginning database migration process.")









# Load into SQL table, config file contains user, passwd, host, and db name.
port = '3306'
table_name = 'daily_basket'
engine = create_engine(f'mysql+mysqlconnector://{config.user}:{config.db_passwd}@{config.host}:{port}/{config.db}')


# Check the connection to the database
connection = mysql.connector.connect(
    host=config.host,
    user=config.user,
    passwd=config.db_passwd,
    db=config.db
)

if connection.is_connected() == True:
    print("Task complete: Connected to database.")
else:
    print("ERROR: Couldn't connect to database.")





cursor = connection.cursor()

# Set wait_timeout for the current session
cursor.execute("SET SESSION wait_timeout=600")
cursor.execute("SET SESSION interactive_timeout=600")

# Wipe the table.
cursor.execute(f"TRUNCATE TABLE {table_name};")
connection.commit()

# Check if the table is empty
cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
row_count = cursor.fetchone()[0]

# Print the result
if row_count == 0:
    print(f"Task complete: '{table_name}' table has been successfully cleared.")
else:
    print(f"ERROR: Failed to clear '{table_name}' table. It still contains {row_count} rows.")





# Insert data in batches
batch_size = 1000  # Adjust the batch size as needed
for start in range(0, len(df_daily), batch_size):
    end = start + batch_size
    batch = df_daily[start:end]
    batch.to_sql(name=table_name, con=engine, if_exists='append', index=False)

print(f"Task complete: Successfully migrated data to '{table_name}' table." )      # Preview below.





# Print a preview of the table.
cursor.execute(f"SELECT * FROM {table_name} LIMIT 10;")

# Fetch the result
table_preview = cursor.fetchall()

# Print each row in the preview
for row in table_preview:
    print(row)





# Close the connection
cursor.close()
connection.close()

print("Update is complete. Connection closed.")