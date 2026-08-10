---
file_format: mystnb
---

# Creating Bars

```{tip}
OneTick Cloud has daily and minute bars precomputed and available in `_BARS` databases.  
Daily OHLCV data with the official closing prices is also available.  
See [Data Retrieval - Bars](./data_retrieval_bars.md) and [Daily OHLCV](../getting_started/daily_OHLCV.ipynb).
```

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Simple OHLCV Bar Creation

Calculate trade aggregation, passing in a dictionary of aggregates into the
{meth}`~onetick.py.Source.agg` method of the trade data source.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

data = data.agg({
    'OPEN': otp.agg.first('PRICE'),
    'HIGH': otp.agg.max('PRICE'),
    'LOW': otp.agg.min('PRICE'),
    'CLOSE': otp.agg.last('PRICE'),
    'VOLUME': otp.agg.sum('SIZE'),
    'COUNT': otp.agg.count(),
})

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Extended OHLCV Bar Creation

Calculate trade aggregation, passing in a dictionary of aggregates
into the {meth}`~onetick.py.Source.agg` method of the trade data source.  
Additional aggregates are defined for the First and Last time of a trade,
and the timestamp of the High Price and Low Price.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

data = data.agg({
    'OPEN': otp.agg.first('PRICE'),
    'HIGH': otp.agg.max('PRICE'),
    'LOW': otp.agg.min('PRICE'),
    'CLOSE': otp.agg.last('PRICE'),
    'VOLUME': otp.agg.sum('SIZE'),
    'COUNT': otp.agg.count(),
    'OPEN_TIME':otp.agg.first_time(),
    'HIGH_TIME':otp.agg.high_time('PRICE'),
    'LOW_TIME':otp.agg.low_time('PRICE'),
    'CLOSE_TIME':otp.agg.last_time(),
})

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Dynamic Bar Creation - 5 Seconds

Calculate custom trade bars, with a bucket interval defined as 5 seconds.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

data = data.agg({
    'OPEN': otp.agg.first('PRICE'),
    'HIGH': otp.agg.max('PRICE'),
    'LOW': otp.agg.min('PRICE'),
    'CLOSE': otp.agg.last('PRICE'),
    'VOLUME': otp.agg.sum('SIZE'),
    'COUNT': otp.agg.count(),
}, bucket_interval=5)

# Return first 1000 Rows
data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Dynamic Bar Creation - 5 Minutes

Calculate custom trade bars, with a bucket interval defined as 5 minutes.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

data = data.agg({
    'OPEN': otp.agg.first('PRICE'),
    'HIGH': otp.agg.max('PRICE'),
    'LOW': otp.agg.min('PRICE'),
    'CLOSE': otp.agg.last('PRICE'),
    'VOLUME': otp.agg.sum('SIZE'),
    'COUNT': otp.agg.count(),
}, bucket_interval=otp.Minute(5))

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Dynamic Bar Creation - Tick Bin

Calculate custom trade bars, with a bucket interval defined as 1000 records (ticks).

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

data = data.agg({
    'OPEN': otp.agg.first('PRICE'),
    'HIGH': otp.agg.max('PRICE'),
    'LOW': otp.agg.min('PRICE'),
    'CLOSE': otp.agg.last('PRICE'),
    'VOLUME': otp.agg.sum('SIZE'),
    'COUNT': otp.agg.count(),
}, bucket_interval=1000, bucket_units='ticks')

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## VWAP Bar Creation

Calculate the Volume Weighted Average Price (VWAP) using the vwap aggregate, outputting 5 minute bars.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

data = data.agg({
    'AVG_PRICE': otp.agg.average('PRICE'),
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
}, bucket_interval=otp.Minute(5))

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## TWAP Bar Creation

Calculate the Time Weighted Average Price (TWAP)
using the {func}`~onetick.py.agg.tw_average` aggregate, outputting 5 minute bars.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

data = data.agg({
    'AVG_PRICE': otp.agg.average('PRICE'),
    'TWAP_PRICE': otp.agg.tw_average('PRICE'),
}, bucket_interval=otp.Minute(5))

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Dynamic Bars for Symbols Across Databases

Retrieve Trades and Calculate Dynamic Bars for Symbols across Databases for the specified time range.  
The initial {class}`otp.DataSource <onetick.py.DataSource>` is defined without specifying the Database or symbol.  
The schema of the Data Source is specified manually.  
Symbols are specified including the Database name, with format ``[Database]::[Symbol]`` e.g. ``LSE::VOD``.

```{code-cell} ipython3

import onetick.py as otp

# Define the Symbol List
sym_list = ['LSE::VOD', 'EURONEXT::AF', 'XETRA::DBK', 'LSE::TSCO',
            'LSE::SHEL', 'EURONEXT::AF', 'LSE::VOD', 'XETRA::DBK']

# Define Data Source, in this case without specifying the Database or symbol name.
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
# Specify Output Fields
trd = trd[['PRICE', 'SIZE', 'TRADE_VENUE', 'BOOK_TYPE', 'TRADE_PERIOD']]

# Filter on Lit Order Book
trd = trd.where(trd['BOOK_TYPE'] == '0')

# Filter on Continuous Trading
trd = trd.where(trd['TRADE_PERIOD'] == '-')

# Aggregates All Trades into 5 Minute Buckets
data = trd.agg({
    'OPEN': otp.agg.first('PRICE'),
    'HIGH': otp.agg.max('PRICE'),
    'LOW': otp.agg.min('PRICE'),
    'CLOSE': otp.agg.last('PRICE'),
    'VOLUME': otp.agg.sum('SIZE'),
    'VWAP': otp.agg.vwap('PRICE', 'SIZE'),
    'COUNT': otp.agg.count()
}, bucket_interval=otp.Minute(5))

# Create a single output, merging all the inputs into a single resultset.
merged = otp.merge([data], symbols=sym_list, identify_input_ts=True, separate_db_name=True)

# Run the query returning the data in the selected timezone
result = otp.run(merged,
                 start=otp.datetime(2024, 1, 3, 8),
                 end=otp.datetime(2024, 1, 4, 16),
                 timezone='Europe/London')
result
```
