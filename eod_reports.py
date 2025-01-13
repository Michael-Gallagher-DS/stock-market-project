import config
import MySQLdb
import sshtunnel
import pandas as pd

sshtunnel.SSH_TIMEOUT = 300.0
sshtunnel.TUNNEL_TIMEOUT = 300.0

# Libraries for email
import smtplib
from email.message import EmailMessage

# Libraries for png conversion
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table

print("Task completed: Libraries imported.")






query_closers_with_rvol = """
SELECT *
FROM daily_sp500
WHERE date = (
    SELECT MAX(date)
    FROM daily_sp500)
  AND (range_pct > 0.8 OR range_pct < 0.2)
  AND rvol > 0.8
  AND adv > 2000000
ORDER BY rvol DESC;
"""

query_etf_closers_with_rvol = """
SELECT *
FROM daily_etf
WHERE date = (
    SELECT MAX(date)
    FROM daily_sp500)
  AND (range_pct > 0.8 OR range_pct < 0.2)
  AND rvol > 0.8
  AND adv > 2000000
ORDER BY rvol DESC;
"""

query_advance_vs_decline = """
SELECT
    SUM(CASE WHEN pct_change > 0 THEN 1 ELSE 0 END) AS Advances,
    SUM(CASE WHEN pct_change < 0 THEN 1 ELSE 0 END) AS Declines,
    SUM(CASE WHEN pct_change = 0 THEN 1 ELSE 0 END) AS NoChange
FROM daily_sp500
WHERE date = (
    SELECT MAX(date)
    FROM daily_sp500
);
"""

query_above_below_ma = """
SELECT
    SUM(CASE WHEN close > ema_9 THEN 1 ELSE 0 END) AS Over_9_EMA,
    SUM(CASE WHEN close < ema_9 THEN 1 ELSE 0 END) AS Under_9_EMA,
    SUM(CASE WHEN close > ema_21 THEN 1 ELSE 0 END) AS Over_21_EMA,
    SUM(CASE WHEN close < ema_21 THEN 1 ELSE 0 END) AS Under_21_EMA,
    SUM(CASE WHEN close > sma_50 THEN 1 ELSE 0 END) AS Over_50_SMA,
    SUM(CASE WHEN close < sma_50 THEN 1 ELSE 0 END) AS Under_50_SMA,
    SUM(CASE WHEN close > sma_200 THEN 1 ELSE 0 END) AS Over_200_SMA,
    SUM(CASE WHEN close < sma_200 THEN 1 ELSE 0 END) AS Under_200_SMA
FROM daily_sp500
WHERE date = (
    SELECT MAX(date)
    FROM daily_sp500
);
"""




### New RVOL, tr_comp, gainers/losers, and up x% over y days.
query_rvol = """
SELECT symbol, rvol, close, pct_change, pct_open_to_close, pct_gap, tr_comp, volume
FROM daily_sp500
WHERE date = (
    SELECT MAX(date)
    FROM daily_sp500
)
ORDER BY rvol desc
LIMIT 10;
"""



query_tr_comp = """
SELECT symbol, tr_comp, close, pct_change, pct_open_to_close, pct_gap, rvol, volume
FROM daily_sp500
WHERE date = (
    SELECT MAX(date)
    FROM daily_sp500
)
ORDER BY tr_comp desc
LIMIT 10;
"""



query_gainers = """
SELECT symbol, close, pct_change, pct_open_to_close, pct_gap, rvol, tr_comp, volume
FROM daily_sp500
WHERE date = (
    SELECT MAX(date)
    FROM daily_sp500
)
ORDER BY pct_open_to_close desc
LIMIT 10;
"""



query_losers = """
SELECT symbol, close, pct_change, pct_open_to_close, pct_gap, rvol, tr_comp, volume
FROM daily_sp500
WHERE date = (
    SELECT MAX(date)
    FROM daily_sp500
)
ORDER BY pct_open_to_close
LIMIT 10;
"""









## Queries for S&P 500 stocks up x% over y number of days.
query_up_20_in_5 = """
WITH recent_data AS (
    SELECT symbol,
           close,
           pct_change,
           pct_open_to_close,
           pct_gap,
           rvol,
           atr,
           adv,
           tr_comp,
           ema_9,
           volume,
           rsi,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS row_num
    FROM daily_sp500
)
SELECT
    r1.symbol,
    ROUND((r1.close / r6.close - 1) * 100, 1) AS pct_change_5,
    r1.close AS close,
    r1.rvol,
    r1.pct_change,
    r1.pct_open_to_close,
    r1.ema_9,
    r1.atr,
    r1.rsi,
    r1.volume,
    r1.adv,
    r1.tr_comp,
    r1.pct_gap
FROM recent_data r1
JOIN recent_data r6 ON r1.symbol = r6.symbol AND r1.row_num = 1 AND r6.row_num = 6
WHERE r1.close / r6.close - 1 > 0.20
ORDER BY ROUND((r1.close / r6.close - 1) * 100, 1) desc;
"""



query_down_20_in_5 = """
WITH recent_data AS (
    SELECT symbol,
           close,
           pct_change,
           pct_open_to_close,
           pct_gap,
           rvol,
           atr,
           adv,
           tr_comp,
           ema_9,
           volume,
           rsi,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS row_num
    FROM daily_sp500
)
SELECT
    r1.symbol,
    ROUND((r1.close / r6.close - 1) * 100, 1) AS pct_change_5,
    r1.close AS close,
    r1.rvol,
    r1.pct_change,
    r1.pct_open_to_close,
    r1.ema_9,
    r1.atr,
    r1.rsi,
    r1.volume,
    r1.adv,
    r1.tr_comp,
    r1.pct_gap
FROM recent_data r1
JOIN recent_data r6 ON r1.symbol = r6.symbol AND r1.row_num = 1 AND r6.row_num = 6
WHERE r1.close / r6.close - 1 < -0.20
ORDER BY ROUND((r1.close / r6.close - 1) * 100, 1);
"""



query_up_50_in_40 = """
WITH recent_data AS (
    SELECT symbol,
           close,
           pct_change,
           pct_open_to_close,
           pct_gap,
           rvol,
           atr,
           adv,
           tr_comp,
           ema_9,
           volume,
           rsi,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS row_num
    FROM daily_sp500
)
SELECT
    r1.symbol,
    ROUND((r1.close / r41.close - 1) * 100, 1) AS pct_change_40,
    r1.close AS close,
    r1.rvol,
    r1.pct_change,
    r1.pct_open_to_close,
    r1.ema_9,
    r1.atr,
    r1.rsi,
    r1.volume,
    r1.adv,
    r1.tr_comp,
    r1.pct_gap
FROM recent_data r1
JOIN recent_data r41 ON r1.symbol = r41.symbol AND r1.row_num = 1 AND r41.row_num = 41
WHERE r1.close / r41.close - 1 > 0.50
ORDER BY ROUND((r1.close / r41.close - 1) * 100, 1) desc;
"""



query_down_50_in_40 = """
WITH recent_data AS (
    SELECT symbol,
           close,
           pct_change,
           pct_open_to_close,
           pct_gap,
           rvol,
           atr,
           adv,
           tr_comp,
           ema_9,
           volume,
           rsi,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS row_num
    FROM daily_sp500
)
SELECT
    r1.symbol,
    ROUND((r1.close / r41.close - 1) * 100, 1) AS pct_change_40,
    r1.close AS close,
    r1.rvol,
    r1.pct_change,
    r1.pct_open_to_close,
    r1.ema_9,
    r1.atr,
    r1.rsi,
    r1.volume,
    r1.adv,
    r1.tr_comp,
    r1.pct_gap
FROM recent_data r1
JOIN recent_data r41 ON r1.symbol = r41.symbol AND r1.row_num = 1 AND r41.row_num = 41
WHERE r1.close / r41.close - 1 < -0.50
ORDER BY ROUND((r1.close / r41.close - 1) * 100, 1);
"""










## Queries for basket stocks up x% over y number of days.
basket_query_up_20_in_5 = """
WITH recent_data AS (
    SELECT symbol,
           close,
           pct_change,
           pct_open_to_close,
           pct_gap,
           rvol,
           atr,
           adv,
           tr_comp,
           ema_9,
           volume,
           rsi,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS row_num
    FROM daily_basket
)
SELECT
    r1.symbol,
    ROUND((r1.close / r6.close - 1) * 100, 1) AS pct_change_5,
    r1.close AS close,
    r1.rvol,
    r1.pct_change,
    r1.pct_open_to_close,
    r1.ema_9,
    r1.atr,
    r1.rsi,
    r1.volume,
    r1.adv,
    r1.tr_comp,
    r1.pct_gap
FROM recent_data r1
JOIN recent_data r6 ON r1.symbol = r6.symbol AND r1.row_num = 1 AND r6.row_num = 6
WHERE r1.close / r6.close - 1 > 0.20
ORDER BY ROUND((r1.close / r6.close - 1) * 100, 1) desc;
"""



basket_query_down_20_in_5 = """
WITH recent_data AS (
    SELECT symbol,
           close,
           pct_change,
           pct_open_to_close,
           pct_gap,
           rvol,
           atr,
           adv,
           tr_comp,
           ema_9,
           volume,
           rsi,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS row_num
    FROM daily_basket
)
SELECT
    r1.symbol,
    ROUND((r1.close / r6.close - 1) * 100, 1) AS pct_change_5,
    r1.close AS close,
    r1.rvol,
    r1.pct_change,
    r1.pct_open_to_close,
    r1.ema_9,
    r1.atr,
    r1.rsi,
    r1.volume,
    r1.adv,
    r1.tr_comp,
    r1.pct_gap
FROM recent_data r1
JOIN recent_data r6 ON r1.symbol = r6.symbol AND r1.row_num = 1 AND r6.row_num = 6
WHERE r1.close / r6.close - 1 < -0.20
ORDER BY ROUND((r1.close / r6.close - 1) * 100, 1);
"""



basket_query_up_50_in_40 = """
WITH recent_data AS (
    SELECT symbol,
           close,
           pct_change,
           pct_open_to_close,
           pct_gap,
           rvol,
           atr,
           adv,
           tr_comp,
           ema_9,
           volume,
           rsi,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS row_num
    FROM daily_basket
)
SELECT
    r1.symbol,
    ROUND((r1.close / r41.close - 1) * 100, 1) AS pct_change_40,
    r1.close AS close,
    r1.rvol,
    r1.pct_change,
    r1.pct_open_to_close,
    r1.ema_9,
    r1.atr,
    r1.rsi,
    r1.volume,
    r1.adv,
    r1.tr_comp,
    r1.pct_gap
FROM recent_data r1
JOIN recent_data r41 ON r1.symbol = r41.symbol AND r1.row_num = 1 AND r41.row_num = 41
WHERE r1.close / r41.close - 1 > 0.50
ORDER BY ROUND((r1.close / r41.close - 1) * 100, 1) desc;
"""



basket_query_down_50_in_40 = """
WITH recent_data AS (
    SELECT symbol,
           close,
           pct_change,
           pct_open_to_close,
           pct_gap,
           rvol,
           atr,
           adv,
           tr_comp,
           ema_9,
           volume,
           rsi,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS row_num
    FROM daily_basket
)
SELECT
    r1.symbol,
    ROUND((r1.close / r41.close - 1) * 100, 1) AS pct_change_40,
    r1.close AS close,
    r1.rvol,
    r1.pct_change,
    r1.pct_open_to_close,
    r1.ema_9,
    r1.atr,
    r1.rsi,
    r1.volume,
    r1.adv,
    r1.tr_comp,
    r1.pct_gap
FROM recent_data r1
JOIN recent_data r41 ON r1.symbol = r41.symbol AND r1.row_num = 1 AND r41.row_num = 41
WHERE r1.close / r41.close - 1 < -0.50
ORDER BY ROUND((r1.close / r41.close - 1) * 100, 1);
"""


print("Task completed: SQL queries complete.")







with sshtunnel.SSHTunnelForwarder(
    (config.ssh_host),
    ssh_username=config.user,
    ssh_password=config.user_passwd,
    remote_bind_address=(config.host, 3306)
) as tunnel:
    connection = MySQLdb.connect(
        user=config.user,
        passwd=config.db_passwd,
        host='127.0.0.1', port=tunnel.local_bind_port,
        db=config.db,
    )

    # Execute query and return DataFrames
    df_closers_with_rvol = pd.read_sql(query_closers_with_rvol, connection)
    df_etf_closers_with_rvol = pd.read_sql(query_etf_closers_with_rvol, connection)
    df_query_advance_vs_decline = pd.read_sql(query_advance_vs_decline, connection)
    df_query_above_below_ma = pd.read_sql(query_above_below_ma, connection)

    df_query_rvol = pd.read_sql(query_rvol, connection)
    df_query_tr_comp = pd.read_sql(query_tr_comp, connection)
    df_query_gainers = pd.read_sql(query_gainers, connection)
    df_query_losers = pd.read_sql(query_losers, connection)

    df_query_up_20_in_5 = pd.read_sql(query_up_20_in_5, connection)
    df_query_down_20_in_5 = pd.read_sql(query_down_20_in_5, connection)
    df_query_up_50_in_40 = pd.read_sql(query_up_50_in_40, connection)
    df_query_down_50_in_40 = pd.read_sql(query_down_50_in_40, connection)

    df_basket_query_up_20_in_5 = pd.read_sql(basket_query_up_20_in_5, connection)
    df_basket_query_down_20_in_5 = pd.read_sql(basket_query_down_20_in_5, connection)
    df_basket_up_50_in_40 = pd.read_sql(basket_query_up_50_in_40, connection)
    df_basket_down_50_in_40 = pd.read_sql(basket_query_down_50_in_40, connection)

    connection.close()

print("Task completed: Dataframes constructed from queries.")









### Save dataframes to two excel files, one for SP500 and one for basket.
writer_sp500 = pd.ExcelWriter('/home/mgallagherdata/db-build/Report files/reports_sp_500.xlsx', engine='xlsxwriter')

# Drop uneeded columns
df_closers_with_rvol_ = df_closers_with_rvol.drop(columns=['date', 'open'], inplace=False)
df_etf_closers_with_rvol_ = df_etf_closers_with_rvol.drop(columns=['date', 'open'], inplace=False)


# Save excel file.
df_query_rvol.to_excel(writer_sp500, sheet_name='Highest RVOL',  index=False)
df_query_tr_comp.to_excel(writer_sp500, sheet_name='Outlier Moves',  index=False)
df_query_gainers.to_excel(writer_sp500, sheet_name='Gainers',  index=False)
df_query_losers.to_excel(writer_sp500, sheet_name='Losers',  index=False)

df_query_up_20_in_5.to_excel(writer_sp500, sheet_name='+20% in 5 sessions',  index=False)
df_query_down_20_in_5.to_excel(writer_sp500, sheet_name='-20% in 5 sessions',  index=False)
df_query_up_50_in_40.to_excel(writer_sp500, sheet_name='+50% in 40 sessions',  index=False)
df_query_down_50_in_40.to_excel(writer_sp500, sheet_name='-50% in 40 sessions',  index=False)

df_closers_with_rvol_.to_excel(writer_sp500, sheet_name='SW Closers (SP500)',  index=False)
df_etf_closers_with_rvol_.to_excel(writer_sp500, sheet_name='SW Closers (ETF)',  index=False)

writer_sp500.close()

print("Task completed: Populated S&P 500 excel file.")





writer_basket = pd.ExcelWriter('/home/mgallagherdata/db-build/Report files/reports_basket.xlsx', engine='xlsxwriter')

df_basket_query_up_20_in_5.to_excel(writer_basket, sheet_name='+20% in 5 sessions',  index=False)
df_basket_query_down_20_in_5.to_excel(writer_basket, sheet_name='-20% in 5 sessions',  index=False)
df_basket_up_50_in_40.to_excel(writer_basket, sheet_name='+50% in 40 sessions',  index=False)
df_basket_down_50_in_40.to_excel(writer_basket, sheet_name='-50% in 40 sessions',  index=False)

writer_basket.close()

print("Task completed: Populated basket excel file.")








### Save df_query_above_below_ma dataframe to a png file.

# Create the plot and adjust its size
fig, ax = plt.subplots(figsize=(10, 4))
ax.axis('tight')
ax.axis('off')

# Create and customize the table
table = ax.table(cellText=df_query_above_below_ma.values,
                 colLabels=df_query_above_below_ma.columns,
                 cellLoc='center',
                 loc='center')

# Adjust font size and cell padding, add title
table.auto_set_font_size(False)
table.set_fontsize(12)
table.scale(1.6, 2)

fig.suptitle('S&P 500 Above/Below Moving Averages', fontsize=20, y=0.7)

# Save the figure.
plt.savefig(r'/home/mgallagherdata/db-build/Report files/png_above_below_ma.png', bbox_inches='tight', dpi=300)
plt.close(fig)









### Save df_query_advance_vs_decline dataframe to a png file.

# Create the plot and adjust its size
fig, ax = plt.subplots(figsize=(10, 4))
ax.axis('tight')
ax.axis('off')

# Create and customize the table
table = ax.table(cellText=df_query_advance_vs_decline.values,
                 colLabels=df_query_advance_vs_decline.columns,
                 cellLoc='center',
                 loc='center')

# Adjust font size and cell padding, add title
table.auto_set_font_size(False)
table.set_fontsize(12)
table.scale(0.5, 2)

fig.suptitle('S&P 500 Winners vs Losers', fontsize=20, y=0.7)

# Save the figure.
plt.savefig(r'/home/mgallagherdata/db-build/Report files/png_advance_vs_decline.png', bbox_inches='tight', dpi=300)
plt.close(fig)

print("Task completed: Images created and saved.")









import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
import datetime

today = datetime.date.today().strftime("%Y-%m-%d")

# Email configuration
sender_email = config.gmail
receiver_email = config.gmail
subject = f"EOD Reports for {today}"
body = "Reports are attached."
password = config.gmail_secret_password

# Create a multipart message and set headers
message = MIMEMultipart()
message["From"] = sender_email
message["To"] = receiver_email
message["Subject"] = subject

# Attach the email body
message.attach(MIMEText(body, "plain"))

# File paths for the CSVs
file_reports_sp_500 = r'/home/mgallagherdata/db-build/Report files/reports_sp_500.xlsx'
file_reports_basket = r'/home/mgallagherdata/db-build/Report files/reports_basket.xlsx'
file_above_below_ma = r'/home/mgallagherdata/db-build/Report files/png_above_below_ma.png'
file_advance_decline = r'/home/mgallagherdata/db-build/Report files/png_advance_vs_decline.png'

# Function to attach files to the email
def attach_file(filepath):
    with open(filepath, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f"attachment; filename= {os.path.basename(filepath)}")
    message.attach(part)

# Attach both CSV files and both png files
attach_file(file_reports_sp_500)
attach_file(file_reports_basket)
attach_file(file_above_below_ma)
attach_file(file_advance_decline)

# Convert message to string
text = message.as_string()

# Send the email
with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, text)

print("Task completed: Email sent with files attached.")
