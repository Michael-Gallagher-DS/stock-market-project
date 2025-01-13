# stock-market-project

## The Database
Created a SQL database containing over 8 years of historical pricing data for 500+ US equities and ETFs. The database includes 15 custom features for technical analysis and is updated daily after market close powered by PythonAnywhere’s task scheduler. The database serves as the backbone for the daily market reports and backtesting tool.

## Automated Daily Reporting
Developed a robust reporting system that delivers market-wide performance insights and actionable trade ideas in a concise, three-part format. The report is automatically emailed at the end of each trading, following the update of the stock market database.

## Backtesting Tool
The backtesting tool utilizes the stock market database for pricing data and technical indicators in conjunction with the backtesting.py library. This framework supports in-depth backtesting of trading strategies and associated metrics (win-rate, P&L, ROI, etc).

## Position Sizing GUI
Designed an easy to use GUI that streamlines trade position sizing and risk management. Relying on only 3 user inputs, this tool increases trade entry speed by 2-3x while eliminating the risk of manual calculation errors.
