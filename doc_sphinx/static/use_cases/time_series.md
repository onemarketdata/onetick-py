---
file_format: mystnb
---

# Time Series

This section contains 14 examples for Time Series using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Accumulative Sum

Calculate the running accumulative aggregation of Trade Size across the defined time period.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]
data = trd.agg({'ACC_SUM': otp.agg.sum('SIZE')}, running=True, all_fields=True)
# Return first 1000 Rows
data = data.limit(1000)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Exponential Moving Average

Query VOD (Vodafone) trade data and compute exponential moving averages.  
Returns running exponential weighted average (EWA) and exponential time-weighted average (ETWA).

```{code-cell} ipython3

import onetick.py as otp

# Define the time interval
start = otp.dt(2024, 1, 3, 8)  # Start: January 3, 2024 at 8:00 AM
end = otp.dt(2024, 1, 4, 16)   # End:   January 4, 2024 at 4:00 PM

# Create the data source for VOD trades
trades = otp.DataSource(
    db='LSE_SAMPLE',          # London Stock Exchange sample database
    tick_type='TRD',          # Trade tick type
    schema_policy='manual',   # Manually specify schema
    schema={'PRICE': float},  # Define the schema for trades: only trade PRICE is needed
)

# Compute running exponential weighted averages
trades_agg = trades.agg({
    'EWA_PRICE': otp.agg.exp_w_average('PRICE', decay=0.01),  # Exponential weighted average with decay 0.01
    'ETWA_PRICE': otp.agg.exp_tw_average('PRICE', decay=300)  # Exponential time-weighted average with decay 300 seconds
}, running=True, all_fields=True)                             # Calculate running aggregates and keep all fields

# Return first 1000 Rows
trades_agg = trades_agg.limit(1000)

# Select the required columns and run the query
result = otp.run(
    trades_agg[['PRICE', 'EWA_PRICE', 'ETWA_PRICE']],  # Select price and both averages
    symbols=['VOD'],                                   # Vodafone stock symbol
    start=start,                                       # Query start time
    end=end,                                           # Query end time
    timezone='UTC',                                    # Use UTC timezone
)

result  # Display results
```

## Multi-day Avg Minute Statistics

Calculate the volume profile across a specified month as average trade size.  
Filtering trades based on trade condition, time range and day of week.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE', 'EXCHANGE', 'COND']]
data = data.where((data['PRICE'] != otp.nan) & (data['SIZE'] != 0))
data = data.character_present(field=data['COND'], characters='O6TU', discard_on_match=True)
data = data.character_present(field=data['COND'], characters='IBCGHLMNPQRVWZ479', discard_on_match=True)
data = data.agg({'SIZE':otp.agg.sum('SIZE')}, bucket_interval=60, bucket_time='start')
data['DAY_NAME'] = data['TIMESTAMP'].dt.day_name('America/New_York')
data = data.where((data['DAY_NAME'] != 'Saturday') & (data['DAY_NAME'] != 'Sunday'))
data['MIN_BAR'] = data['TIMESTAMP'].dt.strftime('%H:%M:%S', 'America/New_York')
data = data.where((data['MIN_BAR'] >= '09:30:00') & (data['MIN_BAR'] < '16:00:00'))
data = data.agg({
    'AVG_SIZE':otp.agg.average('SIZE'),
    'STDDEV_SIZE':otp.agg.stddev('SIZE'),
    'DAY_COUNT':otp.agg.count()
 }, group_by='MIN_BAR', bucket_time='start')
result = otp.run(data,
                 start=otp.dt(2024, 2, 6),
                 end=otp.dt(2024, 3, 7),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Rolling Sum

Calculate the running aggregation of Trade Size across a rolling 60 second time window.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]
data = trd.agg({'ROLLING_SUM': otp.agg.sum('SIZE')},
               bucket_interval=60, running=True, all_fields=True)
# Return first 1000 Rows
data = data.limit(1000)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Rollup of Pre-calculated Quote Bars (QTE_1M)

Rollup of pre-calculated 1 minute quote bars,
using the ``QTE_1M`` tick type and BARS database ``US_COMP_SAMPLE_BARS``.  
The aggregation is more complex as input 1 minute quote bars may not include quotes.  
``skip_tick_if=0`` is used to skip rows where the value is 0, and ``skip_tick_if=otp.nan``, where the value is nan.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='QTE_1M')

# modify timestamp to start, rather than end of bar (TRD_1M is saved with timestamps for the end of bar)
data['NEW_TS'] = data['TIMESTAMP']
data = data.update(if_set={'NEW_TS': data['TIMESTAMP'] - otp.Minute(1)},
                   where=data['TIMESTAMP'] > data['_START_TIME'] - otp.Minute(1))
data = data.update_timestamp('NEW_TS', max_delay_of_new_timestamp=otp.Minute(1))

# Define set of aggregations, as certain records may not values, may need to skip for First and Last aggregates
rollup = data.agg(
    {
        'FIRST_BID_PRICE': otp.agg.first('FIRST_BID_PRICE', skip_tick_if=otp.nan),
        'FIRST_BID_SIZE': otp.agg.first('FIRST_BID_SIZE', skip_tick_if=0),
        'FIRST_BID_TIME': otp.agg.first('FIRST_BID_TIME', large_ints=True, skip_tick_if=0),
        'FIRST_ASK_PRICE': otp.agg.first('FIRST_ASK_PRICE', skip_tick_if=otp.nan),
        'FIRST_ASK_SIZE': otp.agg.first('FIRST_ASK_SIZE', skip_tick_if=0),
        'FIRST_ASK_TIME': otp.agg.first('FIRST_ASK_TIME', large_ints=True, skip_tick_if=0),
        'HIGH_BID': otp.agg.max('HIGH_BID'),
        'LOW_ASK': otp.agg.min('LOW_ASK'),
        'LAST_BID_PRICE': otp.agg.last('LAST_BID_PRICE', skip_tick_if=otp.nan),
        'LAST_BID_SIZE': otp.agg.last('LAST_BID_SIZE', skip_tick_if=0),
        'LAST_BID_TIME': otp.agg.last('LAST_BID_TIME', large_ints=True, skip_tick_if=0),
        'LAST_ASK_PRICE': otp.agg.last('LAST_ASK_PRICE', skip_tick_if=otp.nan),
        'LAST_ASK_SIZE': otp.agg.last('LAST_ASK_SIZE', skip_tick_if=0),
        'LAST_ASK_TIME': otp.agg.last('LAST_ASK_TIME', large_ints=True, skip_tick_if=0),
        'MID_TWAP': otp.agg.average('MID_TWAP'),
        'MID_LAST': otp.agg.last('MID_LAST', skip_tick_if=otp.nan),
        'SPREAD_MIN': otp.agg.min('SPREAD_MIN'),
        'SPREAD_MAX': otp.agg.max('SPREAD_MAX'),
        'SPREAD_TWAP': otp.agg.average('SPREAD_TWAP'),
        'SPREAD_LAST': otp.agg.last('SPREAD_LAST', skip_tick_if=otp.nan),
        'QUOTE_TICK_COUNT': otp.agg.sum('QUOTE_TICK_COUNT'),
        'CLOUD_DB': otp.agg.last('CLOUD_DB')
    },
    # Apply Aggregations across 5 minute buckets
    bucket_interval=otp.Minute(5)
)

result = otp.run(rollup,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Rollup of Pre-calculated Trade Bars (TRD_1M)

Rollup of pre-calculated 1 minute trade bars,
using the ``TRD_1M`` tick type and BARS database ``US_COMP_SAMPLE_BARS``.  
The aggregation is more complex as input 1 minute trade bars may not include trades.  
``skip_tick_if=0`` is used to skip rows where the value is 0, and ``skip_tick_if=otp.nan``, where the value is nan.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')

# modify timestamp to start of minute bar, rather than end of bar (TRD_1M is saved with timestamps for the end of bar)
data['NEW_TS'] = data['TIMESTAMP']
data = data.update(if_set={'NEW_TS': data['TIMESTAMP'] - otp.Minute(1)},
                   where=data['TIMESTAMP'] > data['_START_TIME'] - otp.Minute(1))
data = data.update_timestamp('NEW_TS', max_delay_of_new_timestamp=otp.Minute(1))

# Define set of aggregations, as certain records may not values, may need to skip for First and Last aggregates
rollup = data.agg(
    {
        'FIRST': otp.agg.first('FIRST', skip_tick_if=otp.nan),
        'FIRST_SIZE': otp.agg.first('FIRST_SIZE', skip_tick_if=0),
        'FIRST_TIME': otp.agg.first('FIRST_TIME', large_ints=True, skip_tick_if=0),
        'HIGH': otp.agg.max('HIGH'),
        'LOW': otp.agg.min('LOW'),
        'LAST': otp.agg.last('LAST', skip_tick_if=otp.nan),
        'LAST_SIZE': otp.agg.last('LAST_SIZE', skip_tick_if=0),
        'LAST_TIME': otp.agg.last('LAST_TIME', large_ints=True, skip_tick_if=0),
        'VWAP': otp.agg.vwap(price_column='VWAP',size_column='VOLUME'),
        'TWAP': otp.agg.average('TWAP'),
        'VOLUME': otp.agg.sum('VOLUME'),
        'TRADE_TICK_COUNT': otp.agg.sum('TRADE_TICK_COUNT'),
        'CLOUD_DB': otp.agg.last('CLOUD_DB')
    },
    # Apply Aggregations across 5 minute buckets
    bucket_interval=otp.Minute(5)
)

result = otp.run(rollup,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Simple Moving Averages in Ticks

Calculate a moving average for price based on the a rolling 60 trade count by setting ``bucket_units='ticks'``.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]
data = trd.agg({'MAVG_PRICE': otp.agg.mean('PRICE')},
               bucket_interval=60, bucket_units='ticks', running=True, all_fields=True)
# Return first 1000 Rows
data = data.limit(1000)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Simple Moving Averages in Time

Calculate a 1 and 5 minute moving average for Price.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]
data = trd.agg({'SMA_1M': otp.agg.mean('PRICE')},
               bucket_interval=otp.Minute(1), running=True, all_fields=True)
data = data.agg({'SMA_5M': otp.agg.mean('PRICE')},
                bucket_interval=otp.Minute(5), running=True, all_fields=True)
# Return first 1000 rows
data = data.limit(1000)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Slope & Intercept

Query CSCO (Cisco) trade data and compute linear regression.  
Returns slope and intercept of PRICE vs SIZE relationship over entire interval.

```{code-cell} ipython3

import onetick.py as otp

# Define the data source
data = otp.DataSource(
    db='US_COMP_SAMPLE',  # US composite trades database
    tick_type='TRD',      # Trade tick type
    symbols='CSCO',       # Cisco stock symbol
)

# Compute linear regression over the entire interval
regression = data.agg({
    'reg': otp.agg.linear_regression('PRICE', 'SIZE')  # Linear regression: PRICE (Y) vs SIZE (X)
})

# Run the query
result = otp.run(
    regression,                # Linear regression results
    start=otp.dt(2024, 1, 3),  # Start: January 3, 2024 at midnight
    end=otp.dt(2024, 1, 4),    # End:   January 4, 2024 at midnight
    timezone='UTC',            # Use UTC timezone
)

result  # Display results
```

## Slope & Intercept between Symbols

Query 1-minute bars for ``CSCO`` and ``SPY``, then compute linear regression.  
Returns slope and intercept of ``CSCO`` price vs ``SPY`` price relationship.

```{code-cell} ipython3

import onetick.py as otp

# Define the time interval
start = otp.dt(2024, 1, 3)  # Start: January 3, 2024
end = otp.dt(2024, 1, 4)    # End:   January 4, 2024

# Load 1-minute bars for CSCO and SPY from US_COMP_BARS
csco = otp.DataSource(
    db='US_COMP_BARS',   # US composite bars database
    tick_type='TRD_1M',  # 1-minute trade bars
    symbol='CSCO',       # Cisco stock
)

spy = otp.DataSource(
    db='US_COMP_BARS',   # US composite bars database
    tick_type='TRD_1M',  # 1-minute trade bars
    symbol='SPY',        # S&P 500 ETF
).add_suffix('_SPY')     # Add suffix to SPY columns to avoid naming conflicts

# Join on timestamp using join_by_time (robust to missing/misaligned bars)
joined = otp.join_by_time([csco, spy])  # Time-based join of CSCO and SPY bars

# Compute linear regression: SLOPE and INTERCEPT of CSCO.LAST vs SPY.LAST_SPY
regression = joined.linear_regression('LAST', 'LAST_SPY')  # Linear regression: CSCO price (Y) vs SPY price (X)

# Run the query
result = otp.run(regression, start=start, end=end)  # Execute query for specified time interval
result[['Time', 'SLOPE', 'INTERCEPT']]              # Display Time, Slope, and Intercept columns
```

## Time Zone Spec

Query AAPL trade data with Eastern timezone specification.  
Returns first 1000 trades for specified time interval in `America/New_York` timezone.

```{code-cell} ipython3

import onetick.py as otp

# Define the schema for trade ticks (manual schema policy)
trade_schema = {
    'PRICE': float,   # Trade price
    'SIZE': int,      # Trade size/volume
    'COND': str,      # Trade condition
    'EXCHANGE': str,  # Exchange identifier
}

# Define the time interval
start = otp.dt(2024, 1, 3, 9, 30)  # Start: January 3, 2024 at 9:30 AM
end = otp.dt(2024, 1, 3, 9, 40)    # End:   January 3, 2024 at 9:40 AM

# Create the DataSource for AAPL trades
trades = otp.DataSource(
    db='US_COMP_SAMPLE',     # US composite trades database
    tick_type='TRD',         # Trade tick type
    schema_policy='manual',  # Manually specify schema
    schema=trade_schema,     # Use defined schema above
)

# Limit to 1000 rows
trades = trades.limit(1000)

# Run the query for AAPL in the specified interval and timezone
result = otp.run(
    trades,                       # Trade data source
    symbols=['AAPL'],             # Apple stock symbol
    start=start,                  # Query start time
    end=end,                      # Query end time
    timezone='America/New_York',  # Eastern timezone (IANA format)
)

result  # Display results
```

## Trade Statistics for Symbol List Grouped By Calendar Market Activity

Aggregating Trades for a list of Symbols across Venues, including Calendar Information using ``mkt_activity()``.  
MktActivity returns:
``Rb`` [Pre Market], ``Rr`` [Trading], ``Ra`` [Post Market], ``R1`` [Morning], ``Rx`` [Lunch], ``R2`` [Afternoon].  
Combining Output into a single result set.

```{code-cell} ipython3

import onetick.py as otp

# Define Symbol List with Symbols including the Database using syntax [Db Name]::[Ticker Symbol]
sym_list = [
    'LSE::VOD',
    'EURONEXT::AF',
    'XETRA::DBK',
    'LSE::TSCO',
    'LSE::SHEL'
]

# Define Data Source, in this case without specifying the symbol name.
# As the schema is not yet known, set the schema policy to manual
trd = otp.DataSource(tick_type='TRD', schema_policy='manual')
# Define the output schema
trd.schema.set(
    PRICE=float,
    SIZE=int,
    TRADE_VENUE=str,
    BOOK_TYPE=str,
    TRADE_PERIOD=str
)
trd = trd[['PRICE', 'SIZE', 'TRADE_VENUE', 'BOOK_TYPE', 'TRADE_PERIOD']]

# Add the Symbol to the DataSource
trd['SYMBOL_NAME'] = trd['_SYMBOL_NAME']
# Extract The Calendar Name from the Database component of the Symbol
trd['CALENDAR_NAME'] = 'CLOUD_DB_' + trd['SYMBOL_NAME'].str.extract(r'([^:]+)', rewrite=r'\1')

# Add the Market Activity field, based on the selected Calendar
trd = trd.mkt_activity(calendar_name=trd['CALENDAR_NAME'])

# Aggregates All Trades, grouped by MKT_ACTIVITY value
data = trd.agg({
    'FIRST_PRICE': otp.agg.first('PRICE'),
    'HIGH_PRICE': otp.agg.max('PRICE'),
    'LOW_PRICE': otp.agg.min('PRICE'),
    'LAST_PRICE': otp.agg.last('PRICE'),
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
    'SUM_SIZE': otp.agg.sum('SIZE'),
    'TRADE_COUNT': otp.agg.count()
}, group_by=trd['MKT_ACTIVITY'])

# Create a single output, merging all the inputs into a single resultset.
merged = otp.merge([data], symbols=sym_list, identify_input_ts=True, separate_db_name=True)

# Run the query returning the data in the selected timezone
result = otp.run(merged,
                 start=otp.datetime(2024, 1, 3),
                 end=otp.datetime(2024, 1, 4),
                 timezone='Europe/London')
result
```
