---
file_format: mystnb
---

# Technical Analysis

This section contains 34 examples for Technical Analysis using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Aggressor Volume Imbalance

Aggressor Volume Imbalance from Trade Data.  
Calculated for venues that publish ``AGGRESSOR_SIDE``.  
``BUY_VOLUME`` is the SIZE where AGGRESSOR_SIDE = 'B', ``SELL_VOLUME`` where AGGRESSOR_SIDE = 'S'.  
Volume is aggregated into 1-minute buckets.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for VOD (LSE publishes AGGRESSOR_SIDE)
data = otp.DataSource(db='LSE', tick_type='TRD')
data = data[['SIZE', 'AGGRESSOR_SIDE']]

# Split each trade's size into buy and sell volume based on the aggressor side
data['BUY_VOLUME'] = data.apply(lambda r: r['SIZE'] if r['AGGRESSOR_SIDE'] == 'B' else 0)
data['SELL_VOLUME'] = data.apply(lambda r: r['SIZE'] if r['AGGRESSOR_SIDE'] == 'S' else 0)

# Sum buy, sell and total volume per 1-minute bucket
data = data.agg(
    {
        'VOLUME': otp.agg.sum('SIZE'),
        'BUY_VOLUME': otp.agg.sum('BUY_VOLUME'),
        'SELL_VOLUME': otp.agg.sum('SELL_VOLUME'),
    },
    bucket_interval=otp.Minute(1)
)

# Absolute and percentage imbalance
data['IMBALANCE_VOLUME'] = data['BUY_VOLUME'] - data['SELL_VOLUME']
data['PCNT_IMBALANCE_VOLUME'] = (
    100 * (data['BUY_VOLUME'] - data['SELL_VOLUME']) /
    (data['BUY_VOLUME'] + data['SELL_VOLUME'])
)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 8),
    end=otp.dt(2024, 1, 3, 16),
    timezone='Europe/London',
    symbols='VOD'
)

result
```

## Average True Range (ATR)

Average True Range (ATR) Indicator from Trade Data.  
The three candidate ranges use 1-minute HIGH, LOW and the prevailing price at the start of the window (PRICE_N_BACK):

```text
HL   = HIGH - LOW
H_PC = abs(HIGH - PRICE_N_BACK)
L_PC = abs(LOW - PRICE_N_BACK)
```

The True Range (TR) is the maximum of the three, and ATR is the 14-minute moving average of TR.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE']]

# Rolling 1-minute high, low and prevailing price at the start of the window
data = data.agg(
    {
        'HIGH': otp.agg.max('PRICE'),
        'LOW': otp.agg.min('PRICE'),
        'PRICE_N_BACK': otp.agg.first('PRICE'),
    },
    running=True,
    bucket_interval=otp.Minute(1),
    all_fields=True
)

# The three candidate ranges (H_PC and L_PC are absolute values)
data['HL'] = data['HIGH'] - data['LOW']
data['H_PC'] = data.apply(lambda r: r['HIGH'] - r['PRICE_N_BACK']
                          if r['HIGH'] - r['PRICE_N_BACK'] >= 0
                          else r['PRICE_N_BACK'] - r['HIGH'])
data['L_PC'] = data.apply(lambda r: r['LOW'] - r['PRICE_N_BACK']
                          if r['LOW'] - r['PRICE_N_BACK'] >= 0
                          else r['PRICE_N_BACK'] - r['LOW'])

# True Range is the maximum of the three candidate ranges
def true_range(r):
    if r['HL'] >= r['H_PC'] and r['HL'] >= r['L_PC']:
        return r['HL']
    if r['H_PC'] >= r['L_PC']:
        return r['H_PC']
    return r['L_PC']

data['TR'] = data.apply(true_range)

# ATR: 14-minute moving average of the True Range
data = data.agg(
    {'ATR': otp.agg.average('TR')},
    running=True,
    bucket_interval=otp.Minute(14),
    all_fields=True
)

data = data[['PRICE', 'HIGH', 'LOW', 'PRICE_N_BACK', 'TR', 'ATR']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 30),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Average True Range (ATR) on 1 Min Bars

Average True Range (ATR) Indicator from 1 Minute Trade Bars.  
Returns the Average True Range (ATR) indicator computed on pre-built 1-minute bars.  
The three candidate ranges use the bar HIGH, LOW and the previous bar's LAST (PRIOR_LAST):

```text
HL   = HIGH - LOW
H_PC = abs(HIGH - PRIOR_LAST)
L_PC = abs(LOW - PRIOR_LAST)
```

The True Range (TR) is the maximum of the three, and ATR is the 14-minute moving average of TR.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve 1-minute trade bars for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
data = data[['HIGH', 'LOW', 'LAST']]

# Previous bar's last price (LAST[-1] is the previous bar's LAST)
data['PRIOR_LAST'] = data['LAST'][-1]

# The three candidate ranges (H_PC and L_PC are absolute values)
data['HL'] = data['HIGH'] - data['LOW']
data['H_PC'] = data.apply(lambda r: r['HIGH'] - r['PRIOR_LAST']
                          if r['HIGH'] - r['PRIOR_LAST'] >= 0
                          else r['PRIOR_LAST'] - r['HIGH'])
data['L_PC'] = data.apply(lambda r: r['LOW'] - r['PRIOR_LAST']
                          if r['LOW'] - r['PRIOR_LAST'] >= 0
                          else r['PRIOR_LAST'] - r['LOW'])

# True Range is the maximum of the three candidate ranges
def true_range(r):
    if r['HL'] >= r['H_PC'] and r['HL'] >= r['L_PC']:
        return r['HL']
    if r['H_PC'] >= r['L_PC']:
        return r['H_PC']
    return r['L_PC']

data['TR'] = data.apply(true_range)

# ATR: 14-minute moving average of the True Range
data = data.agg(
    {'ATR': otp.agg.average('TR')},
    running=True,
    bucket_interval=otp.Minute(14),
    all_fields=True
)

data = data[['LAST', 'HIGH', 'LOW', 'PRIOR_LAST', 'TR', 'ATR']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Bollinger Bands

Bollinger Bands from Trade Data.  
A rolling average and rolling standard deviation are calculated across trade ``PRICE``.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

# Rolling 5-minute moving average and standard deviation of PRICE
data = data.agg(
    {
        'MVG_AVG_PRICE': otp.agg.average('PRICE'),
        'MVG_STDDEV_PRICE': otp.agg.stddev('PRICE'),
    },
    running=True,
    bucket_interval=otp.Minute(5),
    all_fields=True
)

# Upper and Lower Bollinger Bands (2 standard deviations from the moving average)
data['BOLLINGER_UPPER_BAND'] = data['MVG_AVG_PRICE'] + 2 * data['MVG_STDDEV_PRICE']
data['BOLLINGER_LOWER_BAND'] = data['MVG_AVG_PRICE'] - 2 * data['MVG_STDDEV_PRICE']

data = data[['PRICE', 'SIZE', 'MVG_AVG_PRICE', 'BOLLINGER_UPPER_BAND', 'BOLLINGER_LOWER_BAND']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Bollinger Bandwidth

Bollinger Bandwidth from Trade Data.  
A rolling average and rolling standard deviation are calculated across trade ``PRICE``.  
The moving window is defined as 5 minutes.  
The Upper and Lower Bollinger Bands are calculated at 2 standard deviations from the average.  
``BOLLINGER_BANDWIDTH`` is 4 times the ``MVG_STDDEV_PRICE`` (the full band width).  
``PCNT_BOLLINGER_BANDWIDTH`` expresses the bandwidth as a percentage of the moving average.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

# Rolling 5-minute moving average and standard deviation of PRICE
data = data.agg(
    {
        'MVG_AVG_PRICE': otp.agg.average('PRICE'),
        'MVG_STDDEV_PRICE': otp.agg.stddev('PRICE'),
    },
    running=True,
    bucket_interval=otp.Minute(5),
    all_fields=True
)

# Bollinger Bands, Bandwidth, and Bandwidth as a percentage of the moving average
data['BOLLINGER_UPPER_BAND'] = data['MVG_AVG_PRICE'] + 2 * data['MVG_STDDEV_PRICE']
data['BOLLINGER_LOWER_BAND'] = data['MVG_AVG_PRICE'] - 2 * data['MVG_STDDEV_PRICE']
data['BOLLINGER_BANDWIDTH'] = 4 * data['MVG_STDDEV_PRICE']
data['PCNT_BOLLINGER_BANDWIDTH'] = 100 * (4 * data['MVG_STDDEV_PRICE'] / data['MVG_AVG_PRICE'])

data = data[['PRICE', 'SIZE', 'MVG_AVG_PRICE', 'BOLLINGER_UPPER_BAND', 'BOLLINGER_LOWER_BAND',
             'BOLLINGER_BANDWIDTH', 'PCNT_BOLLINGER_BANDWIDTH']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Donchian Channels

Donchian Channels from Trade Data.  
The most common period is 20 (here, 20 minutes).  
``UPPER_CHANNEL`` is the rolling maximum price over the last 20 minutes.  
``LOWER_CHANNEL`` is the rolling minimum price over the last 20 minutes.  
``MID_CHANNEL`` is the midpoint between the upper and lower channels.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE']]

# Rolling 20-minute maximum (upper) and minimum (lower) channels
data = data.agg(
    {
        'UPPER_CHANNEL': otp.agg.max('PRICE'),
        'LOWER_CHANNEL': otp.agg.min('PRICE'),
    },
    running=True,
    bucket_interval=otp.Minute(20),
    all_fields=True
)

# Middle channel: midpoint of the upper and lower channels
data['MID_CHANNEL'] = (data['UPPER_CHANNEL'] + data['LOWER_CHANNEL']) / 2

data = data[['PRICE', 'UPPER_CHANNEL', 'LOWER_CHANNEL', 'MID_CHANNEL']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 30),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Donchian Channels on 1 Min Bars

Donchian Channels from 1 Minute Trade Bars.  
The most common period is 20 (here, 20 one-minute bars).  
``UPPER_CHANNEL`` is the rolling maximum of the bar HIGH over the last 20 bars.  
``LOWER_CHANNEL`` is the rolling minimum of the bar LOW over the last 20 bars.  
``MID_CHANNEL`` is the midpoint between the upper and lower channels.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve 1-minute trade bars for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
data = data[['HIGH', 'LOW', 'LAST']]

# Rolling 20-bar maximum HIGH (upper) and minimum LOW (lower) channels
data = data.agg(
    {
        'UPPER_CHANNEL': otp.agg.max('HIGH'),
        'LOWER_CHANNEL': otp.agg.min('LOW'),
    },
    running=True,
    bucket_interval=20,
    bucket_units='ticks',
    all_fields=True
)

# Middle channel: midpoint of the upper and lower channels
data['MID_CHANNEL'] = (data['UPPER_CHANNEL'] + data['LOWER_CHANNEL']) / 2

data = data[['LAST', 'UPPER_CHANNEL', 'LOWER_CHANNEL', 'MID_CHANNEL']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Liquidity Comparison

Liquidity Comparison - Bid-Ask Spread Analysis with Order Book Summary.  
This example analyzes bid-ask VWAP spreads from order book summary.  
Calculates spread in absolute terms and in basis points (bps) relative to mid-price.

```{code-cell} ipython3

import onetick.py as otp

# Get order book summary data from OTQ_CHAIN
# OB_SUMMARY provides best bid/ask prices from order book snapshots
data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_summary(bucket_interval=60, max_depth_shares=1000)
data = data[['BID_VWAP', 'ASK_VWAP']]

# Calculate spread metrics

data['SPREAD'] = data['ASK_VWAP'] - data['BID_VWAP']
data['MID'] = (data['ASK_VWAP'] + data['BID_VWAP']) / 2
data['SPREAD_BPS'] = 10000 * data['SPREAD'] / data['MID']

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3),
    end=otp.dt(2024, 1, 4),
    timezone='UTC',
    symbols='HSBA'
)

result
```

## Market Breadth TRIN Snapshot

Market Breadth - TRIN Snapshot (Daily).  
``TRIN < 1.0`` = volume favoring advances (bullish).  
``TRIN > 1.0`` = volume favoring declines (bearish).  
``TRIN = 1.0`` = neutral.

This snapshot calculates TRIN for the entire trading day by comparing
current price to day's open price for each symbol, weighted by total volume.

```{code-cell} ipython3

import onetick.py as otp

# Get trade data for multiple symbols
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

# Aggregate per symbol: get open, close, and total volume for the day
daily_stats = data.agg({
    'OPEN_PRICE': otp.agg.first('PRICE'),
    'CURRENT_PRICE': otp.agg.last('PRICE'),
    'TOTAL_VOLUME': otp.agg.sum('SIZE')
})

# Classify each symbol as advancing or declining
daily_stats['IS_ADVANCING'] = (daily_stats['CURRENT_PRICE'] > daily_stats['OPEN_PRICE']).astype(int)
daily_stats['IS_DECLINING'] = (daily_stats['CURRENT_PRICE'] < daily_stats['OPEN_PRICE']).astype(int)
daily_stats['ADVANCING_VOLUME'] = daily_stats['TOTAL_VOLUME'] * (daily_stats['CURRENT_PRICE'] > daily_stats['OPEN_PRICE']).astype(int)
daily_stats['DECLINING_VOLUME'] = daily_stats['TOTAL_VOLUME'] * (daily_stats['CURRENT_PRICE'] < daily_stats['OPEN_PRICE']).astype(int)

# Merge per-symbol stats across all symbols for TRIN calculation
daily_stats = otp.merge([daily_stats], separate_db_name=True, identify_input_ts=True,
                        symbols=['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'NVDA', 'META', 'JPM'])

# Aggregate across all symbols to calculate TRIN
trin_result = daily_stats.agg({
    'ADVANCING_SYMBOLS': otp.agg.sum('IS_ADVANCING'),
    'declining_symbols': otp.agg.sum('IS_DECLINING'),
    'ADVANCING_VOLUME': otp.agg.sum('ADVANCING_VOLUME'),
    'DECLINING_VOLUME': otp.agg.sum('DECLINING_VOLUME')
})

# Calculate TRIN: (advancing symbols / declining symbols) / (advancing volume / declining volume)
trin_result['trin'] = (
    (trin_result['ADVANCING_SYMBOLS'] / trin_result['declining_symbols']) /
    (trin_result['ADVANCING_VOLUME'] / trin_result['DECLINING_VOLUME'])
)

result = otp.run(
    trin_result,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 30),
    timezone='America/New_York'
)
result
```

### Market Breadth TRIN Time-Series

Market Breadth - TRIN Time-Series (5-Minute Candles).  
``TRIN < 1.0`` = volume favoring advances (bullish).  
``TRIN > 1.0`` = volume favoring declines (bearish).

This time-series tracks how market breadth evolves throughout the day for each 5-minute candle,
symbols are classified as advancing or declining
based on comparison of their close to the previous candle's close.

```{code-cell} ipython3

import onetick.py as otp

# Get trade data for multiple symbols
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

# Create 5-minute candles: get close and volume per symbol per candle
candles = data.agg({
    'CLOSE_PRICE': otp.agg.last('PRICE'),
    'TOTAL_VOLUME': otp.agg.sum('SIZE')
}, bucket_interval=300)

# Calculate previous candle's close for each symbol
candles['LAST_CLOSE'] = candles['CLOSE_PRICE'][-1]

# Filter to only candles with a previous close
candles = candles.dropna()

# Classify each symbol as advancing or declining
candles['IS_ADVANCING'] = (candles['CLOSE_PRICE'] > candles['LAST_CLOSE']).astype(int)
candles['IS_DECLINING'] = (candles['CLOSE_PRICE'] < candles['LAST_CLOSE']).astype(int)
candles['ADVANCING_VOLUME'] = candles['TOTAL_VOLUME'] * (candles['CLOSE_PRICE'] > candles['LAST_CLOSE']).astype(int)
candles['DECLINING_VOLUME'] = candles['TOTAL_VOLUME'] * (candles['CLOSE_PRICE'] < candles['LAST_CLOSE']).astype(int)

# Merge candles across all symbols for TRIN calculation
candles = otp.merge([candles], separate_db_name=True, identify_input_ts=True,
                    symbols=['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'NVDA', 'META', 'JPM'])

# Aggregate across all symbols per timestamp to calculate TRIN time-series
trin_timeseries = candles.agg({
    'ADVANCING_SYMBOLS': otp.agg.sum('IS_ADVANCING'),
    'DECLINING_SYMBOLS': otp.agg.sum('IS_DECLINING'),
    'ADVANCING_VOLUME': otp.agg.sum('ADVANCING_VOLUME'),
    'DECLINING_VOLUME': otp.agg.sum('DECLINING_VOLUME')
}, bucket_interval=300)

# Calculate TRIN: (advancing symbols / declining symbols) / (advancing volume / declining volume)
trin_timeseries['trin'] = (trin_timeseries['ADVANCING_SYMBOLS'] / trin_timeseries['DECLINING_SYMBOLS']) / \
                          (trin_timeseries['ADVANCING_VOLUME'] / trin_timeseries['DECLINING_VOLUME'])

result = otp.run(
    trin_timeseries,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 30),
    timezone='America/New_York'
)

result
```

## Maximum Drawdown (MDD)

Maximum Drawdown (MDD %) from Trade Data.  
The running high price is tracked from the start of the period.  
``PCNT_DRAWDOWN`` is the percentage difference between the current price and the running high:

```text
PCNT_DRAWDOWN = 100 * (PRICE - RUNNING_HIGH_PRICE) / RUNNING_HIGH_PRICE
```

``MAX_PCNT_DRAWDOWN`` is the running minimum of ``PCNT_DRAWDOWN`` (the largest drawdown so far).

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE']]

# Running high price from the start of the period (cumulative running max)
data = data.agg(
    {'RUNNING_HIGH_PRICE': otp.agg.max('PRICE')},
    running=True,
    all_fields=True
)

# Percentage drawdown from the running high
data['PCNT_DRAWDOWN'] = 100 * (data['PRICE'] - data['RUNNING_HIGH_PRICE']) / data['RUNNING_HIGH_PRICE']

# Maximum drawdown: running minimum of the percentage drawdown
data = data.agg(
    {'MAX_PCNT_DRAWDOWN': otp.agg.min('PCNT_DRAWDOWN')},
    running=True,
    all_fields=True
)

data = data[['PRICE', 'RUNNING_HIGH_PRICE', 'PCNT_DRAWDOWN', 'MAX_PCNT_DRAWDOWN']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 30),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Maximum Drawdown (MDD) on 1 Min Bars

Maximum Drawdown (MDD %) from 1 Minute Trade Bars.  
The running high price is tracked from the start of the period using the bar ``HIGH``.  
``PCNT_DRAWDOWN`` is the percentage difference between the current bar LAST and the running high:

```text
PCNT_DRAWDOWN = 100 * (LAST - RUNNING_HIGH_PRICE) / RUNNING_HIGH_PRICE
```

``MAX_PCNT_DRAWDOWN`` is the running minimum of PCNT_DRAWDOWN (the largest drawdown so far).

```{code-cell} ipython3

import onetick.py as otp

# Retrieve 1-minute trade bars for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
data = data[['HIGH', 'LAST']]

# Running high price from the start of the period (cumulative running max of the bar HIGH)
data = data.agg(
    {'RUNNING_HIGH_PRICE': otp.agg.max('HIGH')},
    running=True,
    all_fields=True
)

# Percentage drawdown from the running high
data['PCNT_DRAWDOWN'] = 100 * (data['LAST'] - data['RUNNING_HIGH_PRICE']) / data['RUNNING_HIGH_PRICE']

# Maximum drawdown: running minimum of the percentage drawdown
data = data.agg(
    {'MAX_PCNT_DRAWDOWN': otp.agg.min('PCNT_DRAWDOWN')},
    running=True,
    all_fields=True
)

data = data[['LAST', 'RUNNING_HIGH_PRICE', 'PCNT_DRAWDOWN', 'MAX_PCNT_DRAWDOWN']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## On-Balance Volume (OBV)

On-Balance Volume (OBV) from Trade Data.  
Retrieves the ``PRICE``, ``SIZE`` and ``PRIOR_PRICE`` (the previous tick's price, ``PRICE[-1]``).

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

# Previous price (PRICE[-1] is the previous tick's price)
data['PRIOR_PRICE'] = data['PRICE'][-1]

# Signed volume based on price direction
def signed_size(r):
    if r['PRICE'] > r['PRIOR_PRICE']:
        return r['SIZE']
    if r['PRICE'] < r['PRIOR_PRICE']:
        return -r['SIZE']
    return 0

data['SIGNED_SIZE'] = data.apply(signed_size)

# On-Balance Volume: cumulative sum of signed volume
# running=True with no bucket_interval accumulates from query start to each tick
data = data.agg(
    {'OBV': otp.agg.sum('SIGNED_SIZE')},
    running=True,
    all_fields=True
)

data = data[['PRICE', 'SIZE', 'SIGNED_SIZE', 'OBV']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### On-Balance Volume (OBV) on 1 Min Bars

On-Balance Volume (OBV) from 1 Minute Trade Bars.  
Retrieves the bar ``LAST``, ``VOLUME`` and the previous bar's ``LAST`` (``PRIOR_LAST``, ``LAST[-1]``).

```text
SIGNED_VOLUME is +VOLUME when LAST > PRIOR_LAST, -VOLUME when LAST < PRIOR_LAST, else 0.
```

``OBV`` is the cumulative sum of ``SIGNED_VOLUME`` across the bars.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve 1-minute trade bars for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
data = data[['LAST', 'VOLUME']]

# Previous bar's last price (LAST[-1] is the previous bar's LAST)
data['PRIOR_LAST'] = data['LAST'][-1]

# Signed volume based on the bar-over-bar price direction
def signed_volume(r):
    if r['LAST'] > r['PRIOR_LAST']:
        return r['VOLUME']
    if r['LAST'] < r['PRIOR_LAST']:
        return -r['VOLUME']
    return 0

data['SIGNED_VOLUME'] = data.apply(signed_volume)

# On-Balance Volume: cumulative sum of signed volume
# running=True with no bucket_interval accumulates from query start to each bar
data = data.agg(
    {'OBV': otp.agg.sum('SIGNED_VOLUME')},
    running=True,
    all_fields=True
)

data = data[['LAST', 'VOLUME', 'SIGNED_VOLUME', 'OBV']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Order Flow Imbalance (OFI)

Order Flow Imbalance (OFI) from Quote Data.  
Order Flow Imbalance (OFI) measures the net change in supply and demand at the
best bid and ask across successive quotes (Cont, Kukanov & Stoikov, 2014).  
As ``US_COMP`` is a composite, the ``NBBO`` tick type is used rather than ``QTE``.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve NBBO quote data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO')
data = data[['BID_PRICE', 'BID_SIZE', 'ASK_PRICE', 'ASK_SIZE']]

# Prior bid and ask prices / sizes (col[-1] is the previous tick's value)
data['PRIOR_BID_PRICE'] = data['BID_PRICE'][-1]
data['PRIOR_BID_SIZE'] = data['BID_SIZE'][-1]
data['PRIOR_ASK_PRICE'] = data['ASK_PRICE'][-1]
data['PRIOR_ASK_SIZE'] = data['ASK_SIZE'][-1]

# Bid flow
def bid_flow(r):
    if r['BID_PRICE'] > r['PRIOR_BID_PRICE']:
        return r['BID_SIZE']
    if r['BID_PRICE'] < r['PRIOR_BID_PRICE']:
        return r['PRIOR_BID_SIZE']
    return r['BID_SIZE'] - r['PRIOR_BID_SIZE']

data['BID_FLOW'] = data.apply(bid_flow)

# Ask flow
def ask_flow(r):
    if r['ASK_PRICE'] > r['PRIOR_ASK_PRICE']:
        return r['ASK_SIZE']
    if r['ASK_PRICE'] < r['PRIOR_ASK_PRICE']:
        return r['PRIOR_ASK_SIZE']
    return r['ASK_SIZE'] - r['PRIOR_ASK_SIZE']

data['ASK_FLOW'] = data.apply(ask_flow)

# Order Flow Imbalance
data['OFI'] = data['BID_FLOW'] - data['ASK_FLOW']

data = data[['BID_PRICE', 'ASK_PRICE', 'BID_FLOW', 'ASK_FLOW', 'OFI']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 30),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Realized Volatility

Realized Volatility (RV) from Trade Data.  
Trades are first bucketed into 1-minute periods, keeping the last price of each period.  
Log returns are calculated as ``LOG(LAST_PRICE / previous LAST_PRICE)``.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE']]

# Divide into 1-minute periods, keeping the last price of each minute
data = data.agg({'LAST_PRICE': otp.agg.last('PRICE')}, bucket_interval=otp.Minute(1))

# Previous minute's last price and the log return
# (LAST_PRICE[-1] is the previous minute's last price)
data['PRIOR_LAST_PRICE'] = data['LAST_PRICE'][-1]
data['LOG_RETURN'] = otp.math.log(data['LAST_PRICE'] / data['PRIOR_LAST_PRICE'])

# Rolling 30-minute standard deviation of the log returns
data = data.agg(
    {'ROLLING_STDDEV_LOG_RETURN': otp.agg.stddev('LOG_RETURN')},
    running=True,
    bucket_interval=otp.Minute(30),
    all_fields=True
)

# Annualize: 252 trading days * 13 thirty-minute periods per day
data['ANNUALIZED_RV'] = data['ROLLING_STDDEV_LOG_RETURN'] * 252 * 13

data = data[['LOG_RETURN', 'ROLLING_STDDEV_LOG_RETURN', 'ANNUALIZED_RV']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Realized Volatility on 1 Min Bars

Realized Volatility (RV) from 1 Minute Trade Bars.  
Log returns are calculated on the bar ``LAST`` as ``LOG(LAST / previous LAST)``.  
A 30-minute rolling standard deviation of the log returns is calculated.  
The result is annualized by multiplying by the number of trading days (252)
and the number of 30-minute periods in a trading day (13).

```{code-cell} ipython3

import onetick.py as otp

# Retrieve 1-minute trade bars for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
data = data[['LAST']]

# Previous bar's last price and the log return (LAST[-1] is the previous bar's LAST)
data['PRIOR_LAST'] = data['LAST'][-1]
data['LOG_RETURN'] = otp.math.log(data['LAST'] / data['PRIOR_LAST'])

# Rolling 30-minute standard deviation of the log returns
data = data.agg(
    {'ROLLING_STDDEV_LOG_RETURN': otp.agg.stddev('LOG_RETURN')},
    running=True,
    bucket_interval=otp.Minute(30),
    all_fields=True
)

# Annualize: 252 trading days * 13 thirty-minute periods per day
data['ANNUALIZED_RV'] = data['ROLLING_STDDEV_LOG_RETURN'] * 252 * 13

data = data[['LOG_RETURN', 'ROLLING_STDDEV_LOG_RETURN', 'ANNUALIZED_RV']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Rate of Change (ROC)

Rate of Change (ROC) Indicator from Trade Data.  
Returns the Rate of Change (ROC) indicator over a lookback period of 7 seconds.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE']]

# Prevailing price at the start of the 7-second window
data = data.agg(
    {'PRICE_N_BACK': otp.agg.first('PRICE')},
    running=True,
    bucket_interval=otp.Second(7),
    all_fields=True
)

# Rate of Change
data['ROC'] = 100 * (data['PRICE'] - data['PRICE_N_BACK']) / data['PRICE_N_BACK']

data = data[['PRICE', 'PRICE_N_BACK', 'ROC']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 30),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Rate of Change (ROC) on 1 Min Bars

Rate of Change (ROC) Indicator from 1 Minute Trade Bars.  
Returns the Rate of Change (ROC) indicator over a lookback of 7 one-minute bars.  
``LAST_N_BACK`` is the bar LAST 7 bars earlier.

```text
 ROC = 100 * (LAST - LAST_N_BACK) / LAST_N_BACK
```

```{code-cell} ipython3

import onetick.py as otp

# Retrieve 1-minute trade bars for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
data = data[['LAST']]

# Bar LAST 7 bars earlier (the first LAST in a running 7-bar window)
data = data.agg(
    {'LAST_N_BACK': otp.agg.first('LAST')},
    running=True,
    bucket_interval=7,
    bucket_units='ticks',
    all_fields=True
)

# Rate of Change
data['ROC'] = 100 * (data['LAST'] - data['LAST_N_BACK']) / data['LAST_N_BACK']

data = data[['LAST', 'LAST_N_BACK', 'ROC']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Rolling Stddev

Rolling Standard Deviation from Trade Data.  
Returns the Rolling Standard Deviation of trade ``PRICE``.  
The period is defined as 5 minutes.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE']]

# Rolling 5-minute standard deviation of PRICE
data = data.agg(
    {'ROLLING_STDDEV_PRICE': otp.agg.stddev('PRICE')},
    running=True,
    bucket_interval=otp.Minute(5),
    all_fields=True
)

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 30),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Rolling Stddev on 1 Min Bars

Rolling Standard Deviation from 1 Minute Trade Bars.  
Returns the rolling standard deviation of the bar ``LAST`` price.  
The period is defined as 5 one-minute bars.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve 1-minute trade bars for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
data = data[['LAST']]

# Rolling 5-bar standard deviation of the bar LAST price
data = data.agg(
    {'ROLLING_STDDEV_PRICE': otp.agg.stddev('LAST')},
    running=True,
    bucket_interval=5,
    bucket_units='ticks',
    all_fields=True
)

data = data[['LAST', 'ROLLING_STDDEV_PRICE']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## RSI

RSI Indicator from Trade Data.  
Returns the RSI together with the RS (Average Gain over Average Loss).  
The previous tick's ``PRICE`` (``PRICE[-1]``) is used to calculate the change in price.  
The change is separated into a GAIN (positive changes) and a LOSS (absolute of negative changes).

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE']]

# Previous price and change in price (PRICE[-1] is the previous tick's price)
data['PRIOR_PRICE'] = data['PRICE'][-1]
data['CHANGE_PRICE'] = data['PRICE'] - data['PRIOR_PRICE']

# Separate the change into gains and losses
data['GAIN'] = data.apply(lambda r: r['CHANGE_PRICE'] if r['CHANGE_PRICE'] > 0 else 0)
data['LOSS'] = data.apply(lambda r: -r['CHANGE_PRICE'] if r['CHANGE_PRICE'] < 0 else 0)

# Rolling 14-minute average gain and loss
data = data.agg(
    {
        'MVG_AVG_GAIN': otp.agg.average('GAIN'),
        'MVG_AVG_LOSS': otp.agg.average('LOSS'),
    },
    running=True,
    bucket_interval=otp.Minute(14),
    all_fields=True
)

# RS and RSI
data['RS'] = data['MVG_AVG_GAIN'] / data['MVG_AVG_LOSS']
data['RSI'] = 100 - (100 / (1 + data['RS']))

data = data[['PRICE', 'RS', 'RSI', 'MVG_AVG_GAIN', 'MVG_AVG_LOSS']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 30),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### RSI on 1 Min Bars

RSI Indicator from 1 Minute Trade Bars.  
Returns the RSI together with the RS (Average Gain over Average Loss).  
The previous bar's ``LAST`` (``LAST[-1]``) is used to calculate the change in price.  
The change is separated into a ``GAIN`` (positive changes) and a ``LOSS`` (absolute of negative changes).  
The gains and losses are averaged across a rolling 14-minute period.

```text
RS  = MVG_AVG_GAIN / MVG_AVG_LOSS
RSI = 100 - (100 / (1 + RS))
```

```{code-cell} ipython3

import onetick.py as otp

# Retrieve 1-minute trade bars for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
data = data[['LAST']]

# Previous bar's last price and the change in price (LAST[-1] is the previous bar's LAST)
data['PRIOR_LAST'] = data['LAST'][-1]
data['CHANGE_LAST'] = data['LAST'] - data['PRIOR_LAST']

# Separate the change into gains and losses
data['GAIN'] = data.apply(lambda r: r['CHANGE_LAST'] if r['CHANGE_LAST'] > 0 else 0)
data['LOSS'] = data.apply(lambda r: -r['CHANGE_LAST'] if r['CHANGE_LAST'] < 0 else 0)

# Rolling 14-minute average gain and loss
data = data.agg(
    {
        'MVG_AVG_GAIN': otp.agg.average('GAIN'),
        'MVG_AVG_LOSS': otp.agg.average('LOSS'),
    },
    running=True,
    bucket_interval=otp.Minute(14),
    all_fields=True
)

# RS and RSI
data['RS'] = data['MVG_AVG_GAIN'] / data['MVG_AVG_LOSS']
data['RSI'] = 100 - (100 / (1 + data['RS']))

data = data[['LAST', 'RS', 'RSI', 'MVG_AVG_GAIN', 'MVG_AVG_LOSS']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Stochastic Oscillator

Stochastic Oscillator from Trade Data.  
Uses a 14-minute rolling Minimum to calculate ``MLOW`` (lowest price traded in the period).  
Uses a 14-minute rolling Maximum to calculate ``MHIGH`` (highest price traded in the period).

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE']]

# Rolling 14-minute minimum (MLOW) and maximum (MHIGH) prices
data = data.agg(
    {
        'MLOW': otp.agg.min('PRICE'),
        'MHIGH': otp.agg.max('PRICE'),
    },
    running=True,
    bucket_interval=otp.Minute(14),
    all_fields=True
)

# %K: position of the current price within the 14-minute high/low range
data['PCNT_K'] = 100 * (data['PRICE'] - data['MLOW']) / (data['MHIGH'] - data['MLOW'])

# %D: 3-minute moving average of %K
data = data.agg(
    {'PCNT_D': otp.agg.average('PCNT_K')},
    running=True,
    bucket_interval=otp.Minute(3),
    all_fields=True
)

data = data[['PRICE', 'MLOW', 'MHIGH', 'PCNT_K', 'PCNT_D']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 30),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Stochastic Oscillator on 1 Min Bars

Stochastic Oscillator from 1 Minute Trade Bars.  
Uses a 14-bar rolling minimum of the bar ``LOW`` to calculate ``MLOW`` (lowest price in the period).  
Uses a 14-bar rolling maximum of the bar ``HIGH`` to calculate ``MHIGH`` (highest price in the period).

```text
%K = 100 * (LAST - MLOW) / (MHIGH - MLOW)
%D = 3-minute moving average of %K
```

```{code-cell} ipython3

import onetick.py as otp

# Retrieve 1-minute trade bars for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
data = data[['HIGH', 'LOW', 'LAST']]

# Rolling 14-bar minimum LOW (MLOW) and maximum HIGH (MHIGH)
data = data.agg(
    {
        'MLOW': otp.agg.min('LOW'),
        'MHIGH': otp.agg.max('HIGH'),
    },
    running=True,
    bucket_interval=14,
    bucket_units='ticks',
    all_fields=True
)

# %K: position of the current bar LAST within the 14-bar high/low range
data['PCNT_K'] = 100 * (data['LAST'] - data['MLOW']) / (data['MHIGH'] - data['MLOW'])

# %D: 3-minute moving average of %K
data = data.agg(
    {'PCNT_D': otp.agg.average('PCNT_K')},
    running=True,
    bucket_interval=otp.Minute(3),
    all_fields=True
)

data = data[['LAST', 'MLOW', 'MHIGH', 'PCNT_K', 'PCNT_D']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Volume Bars

Volume Bars (Fixed Volume Bins) from Trade Data.  
The cumulative volume is calculated across the period and floored into fixed-size bins:

```text
VOL_BIN = FLOOR(cumulative volume / 100000)  (a new bar every 100,000 shares)
```

The day is then aggregated grouped by ``VOL_BIN``, producing OHLC, count and volume per bar.  
Because bars span different time ranges, the start and end time of each bar are also returned.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

# Cumulative volume, then floor into fixed 100,000-share volume bins
data = data.agg({'ACC_VOLUME': otp.agg.sum('SIZE')}, running=True, all_fields=True)
data['VOL_BIN'] = otp.math.floor(data['ACC_VOLUME'] / 100000)

# Aggregate each volume bin into an OHLC bar
data = data.agg(
    {
        'BIN_START': otp.agg.first('Time'),
        'BIN_END': otp.agg.last('Time'),
        'FIRST': otp.agg.first('PRICE'),
        'HIGH': otp.agg.max('PRICE'),
        'LOW': otp.agg.min('PRICE'),
        'LAST': otp.agg.last('PRICE'),
        'TRADE_COUNT': otp.agg.count(),
        'VOLUME': otp.agg.sum('SIZE'),
    },
    group_by=['VOL_BIN']
)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Volume Profile

Volume Profile (Volume Histogram) from Trade Data.  
Retrieves the ``VOLUME`` and ``TRADE_COUNT`` grouped by ``PRICE``.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

# Total volume and trade count for each distinct price level
data = data.agg(
    {
        'VOLUME': otp.agg.sum('SIZE'),
        'TRADE_COUNT': otp.agg.count(),
    },
    group_by=['PRICE']
)

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Volume Profile by Sample Bins

Volume Profile by Sample from Trade Data.  
The price range across the trading day is divided into a fixed number of samples (here 100).

```text
TICK_SIZE = (max price - min price) / (samples - 1) = range / 99.
```

Each trade's price is floored into a ``PRICE_BIN`` by dividing by ``TICK_SIZE``.  
Retrieves the ``VOLUME`` and ``TRADE_COUNT`` grouped by ``PRICE_BIN``.

```{code-cell} ipython3

import onetick.py as otp

SAMPLES = 100

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

# Compute the day's price range with a running (cumulative) min/max so the
# TICK_SIZE is available on every tick without a separate join.
data = data.agg(
    {
        'MIN_PRICE': otp.agg.min('PRICE'),
        'MAX_PRICE': otp.agg.max('PRICE'),
    },
    running=True,
    all_fields=True
)
data['TICK_SIZE'] = (data['MAX_PRICE'] - data['MIN_PRICE']) / (SAMPLES - 1)

# Floor each price into its sample bin
data['PRICE_BIN'] = otp.math.floor(data['PRICE'] / data['TICK_SIZE'])

# Total volume and trade count for each price bin
data = data.agg(
    {
        'VOLUME': otp.agg.sum('SIZE'),
        'TRADE_COUNT': otp.agg.count(),
    },
    group_by=['PRICE_BIN']
)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Volume Profile by Tick Size Bins

Volume Profile by Tick Size from Trade Data.  
A fixed tick size is specified (here 1 cent, 0.01).  
Each trade's price is floored to that tick size to form a ``PRICE_BIN``.  
Retrieves the ``VOLUME`` and ``TRADE_COUNT`` grouped by ``PRICE_BIN``.

```{code-cell} ipython3

import onetick.py as otp

TICK_SIZE = 0.01

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

# Floor each price to the nearest tick-size bin
data['PRICE_BIN'] = otp.math.floor(data['PRICE'] / TICK_SIZE) * TICK_SIZE

# Total volume and trade count for each price bin
data = data.agg(
    {
        'VOLUME': otp.agg.sum('SIZE'),
        'TRADE_COUNT': otp.agg.count(),
    },
    group_by=['PRICE_BIN']
)

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Volume Spike Detection

Volume Spike Detection from Trade Data.  
Volume is aggregated into 1-minute buckets.  
``MAVG_VOLUME`` is the average volume across the last 5 buckets (current + 4 preceding).  
A spike is flagged (``SPIKES = 1``) when the current bucket volume exceeds twice ``MAVG_VOLUME``.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

# Bucket volume, last price and trade count per 1-minute interval
data = data.agg(
    {
        'LAST_PRICE': otp.agg.last('PRICE'),
        'VOLUME': otp.agg.sum('SIZE'),
        'TRADE_COUNT': otp.agg.count(),
    },
    bucket_interval=otp.Minute(1)
)

# Moving average volume over the last 5 buckets (current + 4 preceding)
data = data.agg(
    {'MAVG_VOLUME': otp.agg.average('VOLUME')},
    running=True,
    bucket_interval=5,
    bucket_units='ticks',
    all_fields=True
)

# Flag spikes where volume is more than twice the moving-average volume
data['SPIKES'] = data.apply(lambda r: 1 if r['VOLUME'] > 2 * r['MAVG_VOLUME'] else 0)

data = data[['LAST_PRICE', 'VOLUME', 'TRADE_COUNT', 'MAVG_VOLUME', 'SPIKES']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Volume Spike Detection on 1 Min Bars

Volume Spike Detection from 1 Minute Trade Bars.  
``MAVG_VOLUME`` is the average of the bar ``VOLUME`` across the last 5 bars (current + 4 preceding).  
A spike is flagged (``SPIKES = 1``) when the current bar ``VOLUME`` exceeds twice ``MAVG_VOLUME``.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve 1-minute trade bars for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
data = data[['LAST', 'VOLUME']]

# Moving average volume over the last 5 bars (current + 4 preceding)
data = data.agg(
    {'MAVG_VOLUME': otp.agg.average('VOLUME')},
    running=True,
    bucket_interval=5,
    bucket_units='ticks',
    all_fields=True
)

# Flag spikes where volume is more than twice the moving-average volume
data['SPIKES'] = data.apply(lambda r: 1 if r['VOLUME'] > 2 * r['MAVG_VOLUME'] else 0)

data = data[['LAST', 'VOLUME', 'MAVG_VOLUME', 'SPIKES']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## Volume Surge Indicator

Volume Surge Indicator from Trade Data.  
Volume is aggregated into 1-minute buckets.  
``MAVG_VOLUME`` is the average volume across the last 100 buckets (current + 99 preceding).  
``VOLUME_SURGE = 100 * VOLUME / MAVG_VOLUME`` expresses the current volume relative to the recent average.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

# Bucket volume, last price and trade count per 1-minute interval
data = data.agg(
    {
        'LAST_PRICE': otp.agg.last('PRICE'),
        'VOLUME': otp.agg.sum('SIZE'),
        'TRADE_COUNT': otp.agg.count(),
    },
    bucket_interval=otp.Minute(1)
)

# Moving average volume over the last 100 buckets (current + 99 preceding)
data = data.agg(
    {'MAVG_VOLUME': otp.agg.average('VOLUME')},
    running=True,
    bucket_interval=100,
    bucket_units='ticks',
    all_fields=True
)

# Volume surge: current volume as a percentage of the moving-average volume
data['VOLUME_SURGE'] = 100 * data['VOLUME'] / data['MAVG_VOLUME']

data = data[['LAST_PRICE', 'VOLUME', 'TRADE_COUNT', 'MAVG_VOLUME', 'VOLUME_SURGE']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

### Volume Surge Indicator on 1 Min Bars

Volume Surge Indicator from 1 Minute Trade Bars.  
``MAVG_VOLUME`` is the average of the bar ``VOLUME`` across the last 100 bars (current + 99 preceding).  
``VOLUME_SURGE = 100 * VOLUME / MAVG_VOLUME`` expresses the current bar volume relative to the recent average.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve 1-minute trade bars for CSCO
data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
data = data[['LAST', 'VOLUME', 'TRADE_TICK_COUNT']]

# Moving average volume over the last 100 bars (current + 99 preceding)
data = data.agg(
    {'MAVG_VOLUME': otp.agg.average('VOLUME')},
    running=True,
    bucket_interval=100,
    bucket_units='ticks',
    all_fields=True
)

# Volume surge: current bar volume as a percentage of the moving-average volume
data['VOLUME_SURGE'] = 100 * data['VOLUME'] / data['MAVG_VOLUME']

data = data[['LAST', 'VOLUME', 'TRADE_TICK_COUNT', 'MAVG_VOLUME', 'VOLUME_SURGE']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 9, 30),
    end=otp.dt(2024, 1, 3, 16, 0),
    timezone='America/New_York',
    symbols='CSCO'
)

result
```

## VPIN

Volume Synchronized Probability of Informed Trading (VPIN) from Trade Data.  
Volume bins are formed every 100,000 share.  
``BUY_VOLUME`` is SIZE where AGGRESSOR_SIDE = 'B', ``SELL_VOLUME`` where AGGRESSOR_SIDE = 'S'.  
VPIN is the rolling average of ``BIN_IMBALANCE`` over the last 50 bins.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade data for VOD (LSE publishes AGGRESSOR_SIDE)
data = otp.DataSource(db='LSE', tick_type='TRD')
data = data[['SIZE', 'AGGRESSOR_SIDE']]

# Split size into buy/sell volume by aggressor side
data['BUY_VOLUME'] = data.apply(lambda r: r['SIZE'] if r['AGGRESSOR_SIDE'] == 'B' else 0)
data['SELL_VOLUME'] = data.apply(lambda r: r['SIZE'] if r['AGGRESSOR_SIDE'] == 'S' else 0)

# Cumulative volume, then floor into fixed 100,000-share volume bins
data = data.agg({'ACC_VOLUME': otp.agg.sum('SIZE')}, running=True, all_fields=True)
data['VOL_BIN'] = otp.math.floor(data['ACC_VOLUME'] / 100000)

# Aggregate each volume bin
data = data.agg(
    {
        'BIN_START': otp.agg.first('Time'),
        'BIN_END': otp.agg.last('Time'),
        'TRADE_COUNT': otp.agg.count(),
        'VOLUME': otp.agg.sum('SIZE'),
        'BUY_VOLUME': otp.agg.sum('BUY_VOLUME'),
        'SELL_VOLUME': otp.agg.sum('SELL_VOLUME'),
    },
    group_by=['VOL_BIN']
)

# Order imbalance within each bin (absolute net volume over total volume)
data['DIFF'] = data['BUY_VOLUME'] - data['SELL_VOLUME']
data['ABS_DIFF'] = data.apply(lambda r: r['DIFF'] if r['DIFF'] >= 0 else -r['DIFF'])
data['BIN_IMBALANCE'] = data['ABS_DIFF'] / data['VOLUME']

# VPIN: rolling average of the bin imbalance over the last 50 bins
data = data.agg(
    {'VPIN': otp.agg.average('BIN_IMBALANCE')},
    running=True,
    bucket_interval=50,
    bucket_units='ticks',
    all_fields=True
)

data = data[['VOL_BIN', 'BIN_START', 'BIN_END', 'TRADE_COUNT', 'VOLUME',
             'BUY_VOLUME', 'SELL_VOLUME', 'BIN_IMBALANCE', 'VPIN']]

result = otp.run(
    data,
    start=otp.dt(2024, 1, 3, 8),
    end=otp.dt(2024, 1, 3, 16),
    timezone='Europe/London',
    symbols='VOD'
)

result
```
