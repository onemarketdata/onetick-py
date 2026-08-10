---
file_format: mystnb
---

# Basics

This section contains 14 examples for Basics using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Data Retrieval

Retrieve Exchange trades from the ``US_COMP_SAMPLE`` database for ``CSCO``, across the specified time range.

```{code-cell} ipython3

import onetick.py as otp

# Define Data Source, with Database and Table
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

# Specify Symbol, Time Range, and Time Zone
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 30, 1),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## First N Rows

Retrieve the first 100 trades for ``CSCO`` from the ``US_COMP_SAMPLE`` database, across the specified time range.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

# Return first 100 Rows
data = data.limit(100)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Selecting Fields

Retrieve Trades specifying selected fields as an array of fields from the returned data source.

```{code-cell} ipython3

import onetick.py as otp

# Define the Data Source specifying the Database and Table
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

# Specify the fields to return
data = data[['PRICE', 'SIZE']]

# Return first 100 Rows
data = data.limit(100)

# Run the Query for the defined Symbol and Time Window
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Adding Calculated Fields

Retrieve Trades adding a calculated field to the data source.

```{code-cell} ipython3

import onetick.py as otp

# Define Data Source
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

# Limit Schema
data = data[['PRICE', 'SIZE']]

# Add Calculated Field
data['TRADED_VALUE'] = data['PRICE'] * data['SIZE']

# Return first 100 Rows
data = data.limit(100)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Adding Filters

Retrieve Trades, filtered by exchange and trade size.

```{code-cell} ipython3

import onetick.py as otp

# Define Data Source
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

# Limit Schema
data = data[['PRICE', 'SIZE', 'EXCHANGE']]

# Add Calculated Fields
data['TRADED_VALUE'] = data['PRICE'] * data['SIZE']

# Filter Fields
data = data.where((data['EXCHANGE'] == 'N') & (data['SIZE'] > 100))

# Return first 100 Rows
data = data.limit(100)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Adjusting for Corporate Actions

Retrieve trades with original and corporate action adjusted prices for splits.

```{code-cell} ipython3

import onetick.py as otp

# Defining Data Source
data = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD')

# Restrict Returned Fields
data = data[['PRICE', 'SIZE']]

# Add Fields with Original Price and Size
data['ORIG_PRICE'] = data['PRICE']
data['ORIG_SIZE'] = data['SIZE']

# Adjust PRICE field for historic Corporate Actions
data = data.corp_actions(fields='PRICE',
                         adjustment_date=otp.date(2024, 1, 17),
                         adjust_rule='PRICE',
                         apply_split=True)

# Adjust SIZE field for historic Corporate Actions
data = data.corp_actions(fields='SIZE',
                         adjustment_date=otp.date(2024, 1, 17),
                         adjust_rule='SIZE',
                         apply_split=True)

result = otp.run(data,
                 start=otp.dt(2024, 1, 10),
                 end=otp.dt(2024, 1, 17),
                 timezone='Europe/London',
                 symbols='DKE',
                 symbol_date=otp.dt(2024, 1, 17))
result
```

## Aggregation Statistics

Trade Price Statistics Analysis.  
Computes aggregate price statistics (mean, standard deviation, median, min, max, VWAP, count) for ``LSE`` trades.

```{code-cell} ipython3

import onetick.py as otp

# Create the DataSource for LSE_SAMPLE.TRD trades
data = otp.DataSource(
    db='LSE_SAMPLE',
    tick_type='TRD',
    # Define the schema for trades
    schema_policy='manual',
    schema={'PRICE': float, 'SIZE': int},
)

# Aggregate statistics over the interval
agg = data.agg({
    'MEAN_PRICE': otp.agg.average('PRICE'),
    'STDDEV_PRICE': otp.agg.stddev('PRICE'),
    'MEDIAN_PRICE': otp.agg.median('PRICE'),
    'MAX_PRICE': otp.agg.max('PRICE'),
    'MIN_PRICE': otp.agg.min('PRICE'),
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
    'COUNT_PRICE': otp.agg.count(),
})

# Run the query
result = otp.run(
    agg,
    symbols='VOD',
    start=otp.dt(2024, 1, 3, 8),
    end=otp.dt(2024, 1, 4, 16),
    timezone='UTC',
)
result
```

## Multiple Symbol Retrieval

Retrieve Multiple Symbols by submitting an array of symbols into {func}`otp.run <onetick.py.run>`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')

# Specify the fields to return
data = data[['PRICE', 'SIZE']]

# Return first 100 Rows
data = data.limit(100)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols=['CSCO', 'MSFT'])
result
```

## Identify Trading Days Half Day

Fetch daily bar data for ``HD`` stock, filtered by market activity and exchange,
then retrieve the closing price and activity code for January 2024.

```{code-cell} ipython3

import onetick.py as otp

# Define the data source for daily bars
data = otp.DataSource(
    db='US_COMP_SAMPLE_DAILY',
    tick_type='DAY',
    schema_policy='manual',
    schema={'CLOSE': float, 'EXCHANGE': str}
)

# Market Activity column returns R [Regular], L [Half Day], H [Holiday], W [Weekend]
data = data.mkt_activity('CLOUD_DB_US_COMP')

# Filter for EXCHANGE == ''
data = data.where(data['EXCHANGE'] == '')

# Select only the CLOSE and ACTIVITY_CODE columns
data = data[['CLOSE', 'MKT_ACTIVITY']]

# Return first 100 Rows
data = data.limit(100)

# Run the query for symbol 'HD' and the specified time range
result = otp.run(
    data,
    symbols=['HD'],
    start=otp.dt(2024, 1, 1),
    end=otp.dt(2024, 2, 1),
    timezone='UTC'
)
result
```

## Join Trades to Prevailing Quotes

Retrieve trades joined to prevailing quotes based on an asof join.  
Both Trade and Quote Data sources are defined, and then joined by time using
{func}`otp.join_by_time <onetick.py.join_by_time>`.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]
qte = otp.DataSource(db='US_COMP_SAMPLE', tick_type='QTE')
qte = qte[['BID_PRICE', 'ASK_PRICE']]
data = otp.join_by_time([trd, qte])

# Return first 100 Rows
data = data.limit(100)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Prevailing Price

Retrieve prevailing trade price for ``CSCO`` on database ``US_COMP_SAMPLE`` at a specified timestamp.  
Looking back 1 Day (86400 seconds) in cases where the trade does not occur at the exact timestamp.

```{code-cell} ipython3

import onetick.py as otp

# Define the Data Source with back_to_first_tick
data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', back_to_first_tick=86400)

# Return first 100 Rows
data = data.limit(100)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Query Data Availability Status

Retrieve Completed Data Load Events for ``US_COMP`` Database.  
Returns load completion events from the last 7 days.

```{code-cell} ipython3

import onetick.py as otp

# Calculate time window: last 7 days to now
now = otp.now()
seven_days_ago = now - otp.Day(7)

# Create data source for DB_INFO.PROC_EVENTS
proc_events = otp.DataSource(
    db='DB_INFO',
    tick_type='PROC_EVENTS'
)

# Filter for US_COMP database, successful load events, and events from the last 7 days
proc_events = proc_events.where(
    (proc_events['DB_NAME'] == 'US_COMP') &
    (proc_events['EVENT_NAME'] == 'Load finished')
)

# Run the query for the specified time window and output as a dataframe
result = otp.run(
    proc_events,
    symbols='US_COMP',
    start=seven_days_ago,
    end=now,
    timezone='UTC',
)
result
```

## Ranking

Retrieve trades ranked by price for ``CSCO`` from the ``US_COMP_SAMPLE`` database, across the specified time range.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]
data = data.ranking({'SIZE': 'desc'})

# Return first 100 Rows
data = data.limit(100)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Run SQL

Execute a SQL statement using {class}`otp.SqlQuery <onetick.py.SqlQuery>`.

```{code-cell} ipython3

import onetick.py as otp

sql_statement = """
    SELECT * FROM US_COMP_SAMPLE.TRD
    WHERE SYMBOL_NAME='CSCO'
    and TIMESTAMP >= '2024-01-03 09:30:00 America/New_York'
    and TIMESTAMP < '2024-01-03 09:40:00 America/New_York'
    LIMIT 100
"""

sql_query = otp.SqlQuery(sql_statement)
result = otp.run(sql_query)
result
```
