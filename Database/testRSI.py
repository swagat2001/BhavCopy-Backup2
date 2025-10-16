from tvDatafeed import TvDatafeed, Interval
import pandas as pd

# Login with TradingView Premium
tv = TvDatafeed(username='', password='')
# tv = TvDatafeed()  # works without login for public data

# Stock info
symbol = 'NIFTYNXT50'
exchange = 'NSE'

# Fetch last 200 daily bars
data = tv.get_hist(symbol, exchange, interval=Interval.in_daily, n_bars=200)

# Calculate RSI(40) using Wilder smoothing
delta = data['close'].diff()
gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)

period = 40
avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

rs = avg_gain / avg_loss.replace(0, 1e-10)
data['RSI_40'] = 100 - (100 / (1 + rs))

# Slice last 40 days (underlying + RSI)
last_40_days = data[['close', 'RSI_40']].tail(40)

print(f"\n{symbol} ({exchange}) - Last 40 Days Underlying + RSI(40)")
print(last_40_days)
