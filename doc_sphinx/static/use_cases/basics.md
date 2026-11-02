---
file_format: mystnb
---

# Basics

This section contains examples for Basics using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Data Retrieval

By default all rows and columns are retrieved across the specified time range.  
Specified time zone applies to both the query interval and the results.  
Retrieve Exchange trades from the ``US_COMP_SAMPLE`` database for ``CSCO``.

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

Retrieve only the first 100 trades using {meth}`~onetick.py.Source.limit`
for ``CSCO`` from the ``US_COMP_SAMPLE`` database, across the specified time range.

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
Computes aggregate price statistics (mean, standard deviation, median, min, max, VWAP, TWAP, count) for ``LSE`` trades.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD')

data = trd.agg({
    'MEAN_PRICE': otp.agg.average('PRICE'),
    'STDDEV_PRICE': otp.agg.stddev('PRICE'),
    'MEDIAN_PRICE': otp.agg.median('PRICE'),
    'MAX_PRICE': otp.agg.max('PRICE'),
    'MIN_PRICE': otp.agg.min('PRICE'),
    'TWAP_PRICE': otp.agg.tw_average('PRICE'),
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
    'COUNT_PRICE': otp.agg.count(),
})

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 4, 16),
                 timezone='UTC',
                 symbols='VOD')
result
```

### Aggregation Statistics with Grouping

Results can be aggregated by specifying an aggregation for each field, grouped with `group_by`.  
Computes aggregate price statistics for ``LSE`` trades grouped by `TRADE_CURRENCY`.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD')

data = trd.agg({
    'MEAN_PRICE': otp.agg.average('PRICE'),
    'STDDEV_PRICE': otp.agg.stddev('PRICE'),
    'MEDIAN_PRICE': otp.agg.median('PRICE'),
    'MAX_PRICE': otp.agg.max('PRICE'),
    'MIN_PRICE': otp.agg.min('PRICE'),
    'TWA_PRICE': otp.agg.tw_average('PRICE'),
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
    'COUNT_PRICE': otp.agg.count(),
}, group_by=['TRADE_CURRENCY'])

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 4, 16),
                 timezone='UTC',
                 symbols='VOD')
result
```

### Dynamic Bar Retrieval

Bars can be generated on the fly by aggregating trades into time buckets,
with `bucket_interval` given in seconds.
Additionally a filter is applied using {meth}`~onetick.py.Source.character_present` method with `discard_on_match=True`,
to ensure certain trade conditions are ignored, and do not contribute to the bar generation.  

Common Aggregations for `CSCO` grouped by 5 Minute bar retrieval.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE', 'COND']]

# Exclude the trade conditions that should not contribute to bar generation
trd = trd.character_present(field=trd['COND'], characters='IBCGHLMNPQRVWZ479', discard_on_match=True)

data = trd.agg({
    'FIRST_PRICE': otp.agg.first('PRICE'),
    'HIGH_PRICE': otp.agg.max('PRICE'),
    'LOW_PRICE': otp.agg.min('PRICE'),
    'LAST_PRICE': otp.agg.last('PRICE'),
    'SUM_SIZE': otp.agg.sum('SIZE'),
    'TRADE_COUNT': otp.agg.count(),
}, bucket_interval=300)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 4, 16),
                 timezone='America/New_York',
                 symbols='CSCO')
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

### Wild Card Symbol Selection

Results for a wildcarded list of symbols can be retrieved
by passing a `pattern` to {class}`otp.Symbols <onetick.py.Symbols>` and merging
across the matching symbols, which is the equivalent of SQL's `SYMBOL_NAME LIKE 'BA%'`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD')

# Merge every symbol starting with BA into a single stream.
data = otp.merge([data],
                 symbols=otp.Symbols('LSE_SAMPLE', pattern='BA%', for_tick_type='TRD'),
                 identify_input_ts=True)
data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='UTC')
result
```

## Identify Trading Days, Half Days & Holidays

The {meth}`~onetick.py.Source.mkt_activity` method can be used to identify both trading days and market holidays.  
It adds a `MKT_ACTIVITY` field with a value for the specified calendar.  
The first character represents the day type:
`R` (Regular), `L` (Half Day), `H` (Holiday), `W` (Weekend);
the second represents the general trading period.

Fetch daily bar data for `HD` stock, filtered by market activity and exchange,
then retrieve the closing price and activity code for January 2024.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')
data = data.mkt_activity(calendar_name='CLOUD_DB_US_COMP')
data = data.where(data['EXCHANGE'] == '')
data = data[['CLOSE', 'MKT_ACTIVITY']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 2, 1),
                 timezone='UTC',
                 symbols='HD')
result
```

### Identifying General Trading Sessions

The {meth}`~onetick.py.Source.mkt_activity` method can be used to identify general trading sessions intraday.  
It returns `Rb` (Pre Market), `Rr` (Trading), `Ra` (Post Market), `R1` (Morning), `Rx` (Lunch), `R2` (Afternoon).

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data.mkt_activity(calendar_name='CLOUD_DB_US_COMP')
data = data[['PRICE', 'SIZE', 'COND', 'EXCHANGE', 'MKT_ACTIVITY']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 29, 30),
                 end=otp.dt(2024, 1, 3, 9, 30, 0, 200000),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Join Trades to Prevailing Quotes

Retrieve trades joined to prevailing quotes based on an "as of" join.  
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

## Prevailing Price Retrieval

To return the most recent / prevailing trade price as of a specific time,
set parameter {class}`otp.DataSource(back_to_first_tick=...) <onetick.py.DataSource>`
to the lookback period in seconds (in case where the trade does not occur at the exact timestamp).  
Then take the last tick and query a zero-length window ending at that time.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD',
                      back_to_first_tick=86400,
                      keep_first_tick_timestamp='ORIGINAL_TIMESTAMP')
data = data.limit(2)

as_of = otp.dt(2024, 1, 3, 8, 22, 51, 848000)
result = otp.run(data,
                 start=as_of,
                 end=as_of,
                 timezone='UTC',
                 symbols='VOD')
result
```

### Point in Time Trade Snapshot Across Venue

A specific point in time is selected as the query interval.  
All symbols are retrieved by merging across {class}`otp.Symbols <onetick.py.Symbols>`.  
A lookback of up to 1 day (86400s) via `back_to_first_tick` returns the prevailing trade before the
selected time, per symbol.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', back_to_first_tick=86400)
trd = trd[['PRICE', 'SIZE', 'COND', 'EXCHANGE']]

# Keep only the last (prevailing) tick up to the snapshot time, per symbol.
trd = trd.last()

# Merge the prevailing trade for every symbol into a single stream.
# identify_input_ts adds the SYMBOL_NAME of each tick to the output.
merged = otp.merge([trd],
                   symbols=otp.Symbols('US_COMP_SAMPLE', for_tick_type='TRD'),
                   identify_input_ts=True)

snapshot_time = otp.dt(2024, 1, 3, 12, 30)
result = otp.run(merged,
                 start=snapshot_time,
                 end=snapshot_time,
                 timezone='America/New_York')
result
```

### Combined Point in Time Trade and Quote Snapshot Across a Venue

A specific point in time is selected as the query interval.  
All symbols are retrieved by merging across {class}`otp.Symbols <onetick.py.Symbols>`.  
A lookback of up to 1 day (86400s) via `back_to_first_tick` returns the prevailing trade and quote
before the selected time, per symbol.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD', back_to_first_tick=86400)
trd = trd[['PRICE', 'SIZE']]
trd = trd.last()

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE', back_to_first_tick=86400)
qte = qte[['BID_PRICE', 'ASK_PRICE']]
qte = qte.last()

# Combine the prevailing trade and prevailing quote for each symbol into one tick.
# on='all' cross-joins the (at most one) trade tick with the (at most one) quote tick;
# unlike on='same_size', it degrades gracefully to no output for a symbol
# that lacks a prevailing trade or quote within the lookback window, rather than erroring.
data = otp.join(trd, qte, on='all')

# Merge the combined snapshot for every symbol into a single stream.
# identify_input_ts adds the SYMBOL_NAME of each tick to the output.
data = otp.merge([data],
                 symbols=otp.Symbols('LSE_SAMPLE', for_tick_type='QTE'),
                 identify_input_ts=True)

data = data.limit(1000)

snapshot_time = otp.dt(2024, 1, 3, 12, 30)
result = otp.run(data,
                 start=snapshot_time,
                 end=snapshot_time,
                 timezone='Europe/London')
result
```

## Query Historical Database Availability Status

Retrieve Recent Processing Events for the `US_COMP` Database.  
Database load status is recorded in the `PROC_EVENTS` tick type of the `DB_INFO` database,
one record per processing event.  
The database being queried is passed as the symbol, and filtering on `DB_NAME` restricts the events to that database.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='DB_INFO', tick_type='PROC_EVENTS')
data = data.where(data['DB_NAME'] == 'US_COMP')

result = otp.run(data,
                 # Include events from the last 7 days up to now
                 start=otp.now() - otp.Day(7),
                 end=otp.now(),
                 timezone='UTC',
                 symbols='US_COMP')
result
```

## Ranking

The {meth}`~onetick.py.Source.ranking` method can be used to rank results.  
It adds a `RANKING` field with the position of each tick in the sort order, sorted by given field(s).  
Rows with the same value still get different ranks, matching SQL's `ROW_NUMBER()`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

data = data.ranking({'SIZE': 'desc'})

data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 4, 16),
                 timezone='UTC',
                 symbols='VOD')
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

## Conditional Output with CASE

Conditional fields can be added through {meth}`~onetick.py.Source.apply` method,
where the python function's if/return statements are translated to a OneTick CASE expression.  
Conditions are tested in order and the first match wins; if no
condition matches, the final return provides the equivalent of SQL's "else" clause.

Adding Conditional Field for Trade Class.

```{code-cell} ipython3

import onetick.py as otp

def trade_class(row):
    if row['SIZE'] > 1000:
        return 'LARGE TRADE'
    if row['SIZE'] > 100:
        return 'MEDIUM TRADE'
    return 'SMALL TRADE'

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]
data['TRADE_CLASS'] = data.apply(trade_class)

data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 13),
                 end=otp.dt(2024, 1, 4, 14),
                 timezone='UTC',
                 symbols='CSCO')
result
```

## Limiting Returned Rows With Offset

The set of returned rows can be limited and offset using python slice syntax on the Source.  
`data[5:15]` skips the first 5 ticks and returns the next 10, the equivalent of `LIMIT 10 OFFSET 5`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE', 'EXCHANGE']]
data = data[5:15]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='UTC',
                 symbols='CSCO')
result
```

## Prevailing Quotes for Trades

To retrieve trades joined to prevailing quotes, an "as of" join is specified
using {func}`otp.join_by_time <onetick.py.join_by_time>`,
with the trade source listed first so the trade timestamp leads the join.

```{code-cell} ipython3

import onetick.py as otp

trd = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD')
trd = trd[['PRICE', 'SIZE']]

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte['QTE_TIME'] = qte['TIMESTAMP']
qte = qte[['QTE_TIME', 'BID_PRICE', 'ASK_PRICE']]

data = otp.join_by_time([trd, qte])
data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='UTC',
                 symbols='VOD')
result
```

## Quotes for Specific Trading Sessions

Retrieving Valid Quotes for the specified Market Phase or Trading Session.

Quotes are "as of" joined to the prevailing Market Phase from the `MKT` tick type
using {func}`otp.join_by_time <onetick.py.join_by_time>`, then
filtered to Continuous Trading (`OMD_STATUS == 'T'`).  
Quotes are additionally filtered to a valid size before the join.

Valid Quotes for LSE `VOD` during the Specified Market Phase Trading Session.

```{code-cell} ipython3

import onetick.py as otp

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte = qte[['BID_PRICE', 'ASK_PRICE', 'BID_SIZE', 'ASK_SIZE']]
# Filtering for Valid Quotes
qte = qte.where((qte['BID_SIZE'] > 0) & (qte['ASK_SIZE'] > 0))

mkt = otp.DataSource(db='LSE_SAMPLE', tick_type='MKT')
mkt = mkt[['OMD_STATUS']]

# "as of" join, returning the prevailing market phase for each quote.
data = otp.join_by_time([qte, mkt])

# After the join, filter to T - Continuous Trading.
data = data.where(data['OMD_STATUS'] == 'T')
data = data[['BID_PRICE', 'ASK_PRICE', 'OMD_STATUS']]

data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Top 10 Per Group

The {meth}`~onetick.py.Source.ranking` method can be used to rank results, grouped by a field with `group_by`,
which is the equivalent of SQL's `PARTITION BY`.  
Filtering the `RANKING` field afterwards returns the top N per group.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE', 'TRADE_VENUE']]

# Sort direction is descending, matching "order by SIZE desc".
data = data.ranking({'SIZE': 'desc'}, show_rank_as='order', group_by=['TRADE_VENUE'])
# get top 10 values
data = data.where(data['RANKING'] <= 10)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 4, 16),
                 timezone='UTC',
                 symbols='VOD')
result
```

## Top 25 Per Venue by Daily Traded Value

Query total traded value by venue for a single trading day.  
Returns the top 25 venues ranked by total trading value (`close * volume`).

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='DAY')
data = data[['EXCHANGE', 'CLOSE', 'VOLUME']]

# Notional value of each venue's daily bar, summed per venue below.
data['TRADED_VALUE'] = data['CLOSE'] * data['VOLUME']

# Aggregate trades by exchange.
data = data.agg({'TRADED_VALUE': otp.agg.sum('TRADED_VALUE')}, group_by=['EXCHANGE'])

# Sort by highest traded value first and return only the top 25 venues.
data = data.sort(['TRADED_VALUE'], ascending=False)
data = data.limit(25)

result = otp.run(data,
                 start=otp.dt(2024, 2, 6),
                 end=otp.dt(2024, 2, 7),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Calculate Spread and Mid from a Venue

Venue datasets include top-of-book quotes in the `QTE` tick type.  

```text
SPREAD = ASK_PRICE - BID_PRICE
MID_PRICE = (BID_PRICE + ASK_PRICE) / 2
```

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
data = data.where((data['BID_SIZE'] > 0) & (data['ASK_SIZE'] > 0))

data['MID_PRICE'] = (data['BID_PRICE'] + data['ASK_PRICE']) / 2
data['SPREAD'] = data['ASK_PRICE'] - data['BID_PRICE']
data = data[['BID_PRICE', 'ASK_PRICE', 'MID_PRICE', 'SPREAD']]

data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 3, 16),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

### Calculate Spread and Mid Statistics from a Venue

Venue datasets include top-of-book quotes in the `QTE` tick type.  
`SPREAD` and `MID_PRICE` are computed per quote,
then aggregated across the period using the time weighted average, high and low.

```{code-cell} ipython3

import onetick.py as otp

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte = qte[['BID_PRICE', 'ASK_PRICE', 'BID_SIZE', 'ASK_SIZE']]
qte = qte.where((qte['BID_SIZE'] > 0) & (qte['ASK_SIZE'] > 0))

# Calculate the Mid Price and Spread
qte['MID_PRICE'] = (qte['BID_PRICE'] + qte['ASK_PRICE']) / 2
qte['SPREAD'] = qte['ASK_PRICE'] - qte['BID_PRICE']

data = qte.agg({
    'TW_AVG_MID_PRICE': otp.agg.tw_average('MID_PRICE'),
    'HIGH_MID_PRICE': otp.agg.max('MID_PRICE'),
    'LOW_MID_PRICE': otp.agg.min('MID_PRICE'),
    'TW_AVG_SPREAD': otp.agg.tw_average('SPREAD'),
    'HIGH_SPREAD': otp.agg.max('SPREAD'),
    'LOW_SPREAD': otp.agg.min('SPREAD'),
    'QTE_COUNT': otp.agg.count(),
})

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 3, 16),
                 timezone='Europe/London',
                 symbols='VOD')
result
```
