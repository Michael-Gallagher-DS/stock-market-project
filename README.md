# stock-market-project
Created a SQL database containing over 8 years of historical pricing data for 500+ US equities and ETFs. The database includes 15 custom features for technical analysis and is updated daily after market close powered by PythonAnywhere’s task scheduler. The database serves as the backbone for the automated reporing and backtesting tools.

## Database
A comprehensive financial database powered by MySQL and Python, housing 8+ years of historical market data for more than 500 US stocks and ETFs. The system leverages PythonAnywhere's scheduling capabilities to refresh the data after each trading day.

## Automated Daily Reporting
Developed a robust reporting system that delivers market-wide performance insights and actionable trade ideas in a concise, three-part format. The report is automatically emailed at the end of each trading, following the update of the stock market database.

## Backtesting Tool
The backtesting tool utilizes the stock market database for pricing data and technical indicators in conjunction with the backtesting.py library. This framework supports in-depth backtesting of trading strategies and associated metrics (win-rate, P&L, ROI, etc).

## Position Sizing GUI
Designed an easy to use GUI that streamlines trade position sizing and risk management. Relying on only 3 user inputs, this tool increases trade entry speed by 2-3x while eliminating the risk of manual calculation errors.
