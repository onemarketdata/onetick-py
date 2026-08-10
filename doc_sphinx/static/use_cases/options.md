---
file_format: mystnb
---

# Options

This section contains 16 examples for Options using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Options Greeks

Retrieves end-of-day options data for ``AAPL`` contracts from the US Options EOD sample.  
Includes Greeks (delta, gamma, theta, vega), implied volatility, open interest, volume,
bid/ask close prices, and underlying price for a single trading day.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve the end-of-day (DAY) records for all AAPL option contracts.
# Symbols are selected with a pattern against the US_OPTIONS_EOD_SAMPLE database.
options_day = otp.DataSource(db='US_OPTIONS_EOD_SAMPLE', tick_type='DAY')

# Merge every matching AAPL contract into a single stream.
merged = otp.merge(
    [options_day],
    symbols=otp.Symbols('US_OPTIONS_EOD_SAMPLE', pattern='AAPL%')
)

# Return first 1000 rows
merged = merged.limit(1000)
result = otp.run(
    merged,
    start=otp.dt(2025, 1, 3),
    end=otp.dt(2025, 1, 4),
    timezone='America/New_York'
)
result
```

## Options Trades

Retrieves options trade data for a specific ``AAPL`` contract from the US Options sample feed.  
The symbol encodes the underlying, expiry, call/put indicator and strike price.

```python
import onetick.py as otp

# Define the trade data source for a single option contract.
data = otp.DataSource(db='US_OPTIONS_SAMPLE', tick_type='TRD')

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2025, 1, 2),
    end=otp.dt(2025, 1, 4, 16),
    timezone='UTC',
    symbols='AAPL  250103C00155000'   # Symbol has Underlying, Expiry, Call/Put and Strike
)
result
```

{.dataframe}
|    |                       Time | PRICE | SIZE | EXCHANGE | TRADE_TYPE | TRADE_TYPE_EXT | TRADE_ID   | OMDSEQ |        DELETED_TIME | TICK_STATUS |
|---:|---------------------------:|------:|-----:|---------:|-----------:|---------------:|-----------:|-------:|--------------------:|------------:|
|  0 | 2025-01-03 16:09:05.053000 | 87.91 |    6 |        S |          f |           MLET | 003988253e |    147 | 1970-01-01 00:00:00 |           0 |
|  1 | 2025-01-03 16:09:05.153000 | 87.92 |    7 |        E |          f |           MLET | 00beb5253e |     89 | 1970-01-01 00:00:00 |           0 |
|  2 | 2025-01-03 16:09:05.153000 | 87.95 |    3 |        X |          f |           MLET | 00c3b5253e |     90 | 1970-01-01 00:00:00 |           0 |
|  3 | 2025-01-03 16:09:05.153000 | 87.92 |    7 |        W |          f |           MLET | 00c4b5253e |    164 | 1970-01-01 00:00:00 |           0 |
|  4 | 2025-01-03 16:09:05.286000 | 87.93 |   12 |        J |          g |           MLAT | 000bdb253e |     60 | 1970-01-01 00:00:00 |           0 |
|  5 | 2025-01-03 19:09:30.778000 | 87.84 |    2 |        C |          g |           MLAT | 0001936f7d |     33 | 1970-01-01 00:00:00 |           0 |

## OPRA Call and Put Volume & Open Interest Summary by Strike Price

OPRA Daily data is retrieved from the ``US_OPTIONS_EOD`` database.  
Here the sample database ``US_OPTIONS_EOD_SAMPLE`` is used.  
Daily Volume and Open Interest are split by Calls and Puts and grouped by Strike Price for a specified underlying.  
The daily ``DAY`` records are joined by time to the static ``STAT`` records to obtain the Expiration Date,
Strike Price and Call/Put Indicator.  
A lookback of up to 1 day (86400s) is used on the static data so the
prevailing static record is picked up.  
Volume and Open Interest are divided into Call and Put buckets,
then summed across the day, grouped by Underlying Symbol and Strike Price.

```{code-cell} ipython3

import onetick.py as otp

# Daily options data for all AAPL contracts.
day = otp.DataSource(db='US_OPTIONS_EOD_SAMPLE', tick_type='DAY')
day = day[['VOLUME', 'OPEN_INT']]

# Static (reference) data for the same contracts, looking back up to 1 day for the prevailing record.
stat = otp.DataSource(db='US_OPTIONS_EOD_SAMPLE', tick_type='STAT', back_to_first_tick=86400)
stat = stat[['UNDERLYING_SYMBOL', 'EXPIRATION_DATE', 'STRIKE_PRICE', 'CALL_PUT_IND']]

# Join each daily tick to the prevailing static record (asof join).
joined = otp.join_by_time([day, stat])

# Split Volume and Open Interest into Call and Put buckets based on the Call/Put Indicator.
joined['CALL_VOLUME'] = joined.apply(lambda t: t['VOLUME'] if t['CALL_PUT_IND'] == 'C' else 0)
joined['PUT_VOLUME'] = joined.apply(lambda t: t['VOLUME'] if t['CALL_PUT_IND'] == 'P' else 0)
joined['CALL_OPEN_INT'] = joined.apply(lambda t: t['OPEN_INT'] if t['CALL_PUT_IND'] == 'C' else 0)
joined['PUT_OPEN_INT'] = joined.apply(lambda t: t['OPEN_INT'] if t['CALL_PUT_IND'] == 'P' else 0)

# Sum across the day, grouped by Underlying Symbol and Strike Price.
summary = joined.agg(
    {
        'CONTRACT_COUNT': otp.agg.count(),
        'CALL_VOLUME': otp.agg.sum('CALL_VOLUME'),
        'PUT_VOLUME': otp.agg.sum('PUT_VOLUME'),
        'VOLUME': otp.agg.sum('VOLUME'),
        'CALL_OPEN_INT': otp.agg.sum('CALL_OPEN_INT'),
        'PUT_OPEN_INT': otp.agg.sum('PUT_OPEN_INT'),
        'OPEN_INT': otp.agg.sum('OPEN_INT'),
    },
    group_by=['UNDERLYING_SYMBOL', 'STRIKE_PRICE']
)

# Merge every matching AAPL contract into a single stream.
merged = otp.merge(
    [summary],
    symbols=otp.Symbols('US_OPTIONS_EOD_SAMPLE', pattern='AAPL %')
)

result = otp.run(
    merged,
    start=otp.dt(2025, 1, 3),
    end=otp.dt(2025, 1, 4),
    timezone='America/New_York'
)
result
```

## OPRA Call and Put Volume & Open Interest Summary by Expiration

OPRA Daily data is retrieved from the ``US_OPTIONS_EOD`` database.  
Here the sample database ``US_OPTIONS_EOD_SAMPLE`` is used.  
Daily Volume and Open Interest are split by Calls and Puts and grouped by Expiration Date for a specified underlying.  
The daily ``DAY`` records are joined by time to the static ``STAT`` records to obtain the Expiration Date,
Strike Price and Call/Put Indicator.  
A lookback of up to 1 day (86400s) is used on the static data so the
prevailing static record is picked up.  
Volume and Open Interest are divided into Call and Put buckets,
then summed across the day, grouped by Underlying Symbol and Expiration Date.

```{code-cell} ipython3

import onetick.py as otp

# Daily options data for all AAPL contracts.
day = otp.DataSource(db='US_OPTIONS_EOD_SAMPLE', tick_type='DAY')
day = day[['VOLUME', 'OPEN_INT']]

# Static (reference) data for the same contracts, looking back up to 1 day for the prevailing record.
stat = otp.DataSource(db='US_OPTIONS_EOD_SAMPLE', tick_type='STAT', back_to_first_tick=86400)
stat = stat[['UNDERLYING_SYMBOL', 'EXPIRATION_DATE', 'STRIKE_PRICE', 'CALL_PUT_IND']]

# Join each daily tick to the prevailing static record (asof join).
joined = otp.join_by_time([day, stat])

# Split Volume and Open Interest into Call and Put buckets based on the Call/Put Indicator.
joined['CALL_VOLUME'] = joined.apply(lambda t: t['VOLUME'] if t['CALL_PUT_IND'] == 'C' else 0)
joined['PUT_VOLUME'] = joined.apply(lambda t: t['VOLUME'] if t['CALL_PUT_IND'] == 'P' else 0)
joined['CALL_OPEN_INT'] = joined.apply(lambda t: t['OPEN_INT'] if t['CALL_PUT_IND'] == 'C' else 0)
joined['PUT_OPEN_INT'] = joined.apply(lambda t: t['OPEN_INT'] if t['CALL_PUT_IND'] == 'P' else 0)

# Sum across the day, grouped by Underlying Symbol and Expiration Date.
summary = joined.agg(
    {
        'CONTRACT_COUNT': otp.agg.count(),
        'CALL_VOLUME': otp.agg.sum('CALL_VOLUME'),
        'PUT_VOLUME': otp.agg.sum('PUT_VOLUME'),
        'VOLUME': otp.agg.sum('VOLUME'),
        'CALL_OPEN_INT': otp.agg.sum('CALL_OPEN_INT'),
        'PUT_OPEN_INT': otp.agg.sum('PUT_OPEN_INT'),
        'OPEN_INT': otp.agg.sum('OPEN_INT'),
    },
    group_by=['UNDERLYING_SYMBOL', 'EXPIRATION_DATE']
)

# Merge every matching AAPL contract into a single stream.
merged = otp.merge(
    [summary],
    symbols=otp.Symbols('US_OPTIONS_EOD_SAMPLE', pattern='AAPL %')
)

result = otp.run(
    merged,
    start=otp.dt(2025, 1, 3),
    end=otp.dt(2025, 1, 4),
    timezone='America/New_York'
)
result
```

## OPRA Call and Put Volume across an intra-day time range by Expiry

OPRA Trade data is retrieved from the ``US_OPTIONS`` database.  
Here the sample database ``US_OPTIONS_SAMPLE`` is used.  
Period Volume is split by Calls and Puts and grouped by Expiration Date for a specified underlying.  
Trades are joined by time to the static ``STAT`` records to obtain the Expiration Date, Strike Price and
Call/Put Indicator.  
A lookback of up to 1 day (86400s) is used on the static data so the prevailing
static record is picked up.  
Volume is aggregated from Trade Size and divided into Call and Put buckets,
then summed across the period, grouped by Underlying Symbol and Expiration Date.

```python
import onetick.py as otp

# Intraday options trades for all AAPL contracts.
trd = otp.DataSource(db='US_OPTIONS_SAMPLE', tick_type='TRD')
trd = trd[['SIZE']]

# Static (reference) data for the same contracts, looking back up to 1 day for the prevailing record.
stat = otp.DataSource(db='US_OPTIONS_SAMPLE', tick_type='STAT', back_to_first_tick=86400)
stat = stat[['UNDERLYING_SYMBOL', 'EXPIRATION_DATE', 'STRIKE_PRICE', 'CALL_PUT_IND']]

# Join each trade to the prevailing static record (asof join).
joined = otp.join_by_time([trd, stat])

# Trade Size is the traded volume; split it into Call and Put buckets.
joined['VOLUME'] = joined['SIZE']
joined['CALL_VOLUME'] = joined.apply(lambda t: t['SIZE'] if t['CALL_PUT_IND'] == 'C' else 0)
joined['PUT_VOLUME'] = joined.apply(lambda t: t['SIZE'] if t['CALL_PUT_IND'] == 'P' else 0)

# Sum across the period, grouped by Underlying Symbol and Expiration Date.
summary = joined.agg(
    {
        'CONTRACT_COUNT': otp.agg.count(),
        'CALL_VOLUME': otp.agg.sum('CALL_VOLUME'),
        'PUT_VOLUME': otp.agg.sum('PUT_VOLUME'),
        'VOLUME': otp.agg.sum('VOLUME'),
    },
    group_by=['UNDERLYING_SYMBOL', 'EXPIRATION_DATE']
)

# Merge every matching AAPL contract into a single stream.
merged = otp.merge(
    [summary],
    symbols=otp.Symbols('US_OPTIONS_SAMPLE', pattern='AAPL %')
)

result = otp.run(
    merged,
    start=otp.dt(2025, 1, 3, 10),
    end=otp.dt(2025, 1, 3, 12),
    timezone='America/New_York'
)
result
```

{.dataframe}
|     |                Time | UNDERLYING_SYMBOL | EXPIRATION_DATE | CONTRACT_COUNT | CALL_VOLUME | PUT_VOLUME | VOLUME |
|----:|--------------------:|------------------:|----------------:|---------------:|------------:|-----------:|-------:|
|   0 | 2025-01-03 12:00:00 |              AAPL |        20250103 |              1 |           2 |          0 |      2 |
|   1 | 2025-01-03 12:00:00 |              AAPL |        20250103 |              1 |          10 |          0 |     10 |
|   2 | 2025-01-03 12:00:00 |              AAPL |        20250103 |              5 |          35 |          0 |     35 |
|   3 | 2025-01-03 12:00:00 |              AAPL |        20250103 |              2 |           4 |          0 |      4 |
|   4 | 2025-01-03 12:00:00 |              AAPL |        20250103 |              1 |           1 |          0 |      1 |
| ... |                 ... |               ... |             ... |            ... |         ... |        ... |    ... |
| 811 | 2025-01-03 12:00:00 |              AAPL |        20270115 |              1 |           0 |          3 |      3 |
| 812 | 2025-01-03 12:00:00 |              AAPL |        20270115 |             49 |           0 |        528 |    528 |
| 813 | 2025-01-03 12:00:00 |              AAPL |        20270115 |              3 |           0 |          7 |      7 |
| 814 | 2025-01-03 12:00:00 |              AAPL |        20270115 |              2 |           0 |          2 |      2 |
| 815 | 2025-01-03 12:00:00 |              AAPL |        20270115 |              1 |           0 |          6 |      6 |

816 rows x 7 columns

## OPRA Call and Put Volume across an intra-day time range by Strike Price

OPRA Trade data is retrieved from the ``US_OPTIONS`` database.  
Here the sample database ``US_OPTIONS_SAMPLE`` is used.  
Period Volume is split by Calls and Puts and grouped by Strike Price for a specified underlying.  
Trades are joined by time to the static ``STAT`` records to obtain the Expiration Date, Strike Price and
Call/Put Indicator.  
A lookback of up to 1 day (86400s) is used on the static data so the prevailing
static record is picked up.  
Volume is aggregated from Trade Size and divided into Call and Put buckets,
then summed across the period, grouped by Underlying Symbol and Strike Price.

```python
import onetick.py as otp

# Intraday options trades for all AAPL contracts.
trd = otp.DataSource(db='US_OPTIONS_SAMPLE', tick_type='TRD')
trd = trd[['SIZE']]

# Static (reference) data for the same contracts, looking back up to 1 day for the prevailing record.
stat = otp.DataSource(db='US_OPTIONS_SAMPLE', tick_type='STAT', back_to_first_tick=86400)
stat = stat[['UNDERLYING_SYMBOL', 'EXPIRATION_DATE', 'STRIKE_PRICE', 'CALL_PUT_IND']]

# Join each trade to the prevailing static record (asof join).
joined = otp.join_by_time([trd, stat])

# Trade Size is the traded volume; split it into Call and Put buckets.
joined['VOLUME'] = joined['SIZE']
joined['CALL_VOLUME'] = joined.apply(lambda t: t['SIZE'] if t['CALL_PUT_IND'] == 'C' else 0)
joined['PUT_VOLUME'] = joined.apply(lambda t: t['SIZE'] if t['CALL_PUT_IND'] == 'P' else 0)

# Sum across the period, grouped by Underlying Symbol and Strike Price.
summary = joined.agg(
    {
        'CONTRACT_COUNT': otp.agg.count(),
        'CALL_VOLUME': otp.agg.sum('CALL_VOLUME'),
        'PUT_VOLUME': otp.agg.sum('PUT_VOLUME'),
        'VOLUME': otp.agg.sum('VOLUME'),
    },
    group_by=['UNDERLYING_SYMBOL', 'STRIKE_PRICE']
)

# Merge every matching AAPL contract into a single stream.
merged = otp.merge(
    [summary],
    symbols=otp.Symbols('US_OPTIONS_SAMPLE', pattern='AAPL %')
)

result = otp.run(
    merged,
    start=otp.dt(2025, 1, 3, 10),
    end=otp.dt(2025, 1, 3, 12),
    timezone='America/New_York'
)
result
```

{.dataframe}
|     |                Time | UNDERLYING_SYMBOL | STRIKE_PRICE | CONTRACT_COUNT | CALL_VOLUME | PUT_VOLUME | VOLUME |
|----:|--------------------:|------------------:|-------------:|---------------:|------------:|-----------:|-------:|
|   0 | 2025-01-03 12:00:00 |              AAPL |        140.0 |              1 |           2 |          0 |      2 |
|   1 | 2025-01-03 12:00:00 |              AAPL |        150.0 |              1 |          10 |          0 |     10 |
|   2 | 2025-01-03 12:00:00 |              AAPL |        155.0 |              5 |          35 |          0 |     35 |
|   3 | 2025-01-03 12:00:00 |              AAPL |        160.0 |              2 |           4 |          0 |      4 |
|   4 | 2025-01-03 12:00:00 |              AAPL |        170.0 |              1 |           1 |          0 |      1 |
| ... |                 ... |               ... |          ... |            ... |         ... |        ... |    ... |
| 811 | 2025-01-03 12:00:00 |              AAPL |        185.0 |              1 |           0 |          3 |      3 |
| 812 | 2025-01-03 12:00:00 |              AAPL |        200.0 |             49 |           0 |        528 |    528 |
| 813 | 2025-01-03 12:00:00 |              AAPL |        210.0 |              3 |           0 |          7 |      7 |
| 814 | 2025-01-03 12:00:00 |              AAPL |        240.0 |              2 |           0 |          2 |      2 |
| 815 | 2025-01-03 12:00:00 |              AAPL |        250.0 |              1 |           0 |          6 |      6 |

816 rows x 7 columns

## OPRA Daily Volume, Open Interest and Greeks for Selected Underlying and Expiry

OPRA Daily data is retrieved from the ``US_OPTIONS_EOD`` database.  
Here the sample database ``US_OPTIONS_EOD_SAMPLE`` is used.  
Retrieves end-of-day pricing, Volume, Open Interest and Greeks (Implied Volatility, Delta, Gamma, Theta and Vega),
together with static data including Underlying Symbol, Strike Price, Expiration Date and Call/Put Indicator.  
Filtered for the ``AAPL`` underlying and expiration date 20270115.  
The daily ``DAY`` records are joined by time to the static ``STAT`` records.  
Because the static data may have been
published at a different time to the daily data, a lookback of up to 1 day (86400s) is used so the prevailing
static record is identified.

```{code-cell} ipython3

import onetick.py as otp

# Daily options data for all AAPL contracts.
day = otp.DataSource(db='US_OPTIONS_EOD_SAMPLE', tick_type='DAY')
day = day[['VOLUME', 'OPEN_INT', 'UNDERLYING_PRICE', 'BID_CLOSE', 'ASK_CLOSE',
           'IMP_VOLATILITY', 'DELTA', 'GAMMA', 'THETA', 'VEGA']]

# Static (reference) data for the same contracts, looking back up to 1 day for the prevailing record.
stat = otp.DataSource(db='US_OPTIONS_EOD_SAMPLE', tick_type='STAT', back_to_first_tick=86400)
stat = stat[['UNDERLYING_SYMBOL', 'EXPIRATION_DATE', 'STRIKE_PRICE', 'CALL_PUT_IND']]

# Restrict the static records to the selected expiration.
stat = stat.where(stat['EXPIRATION_DATE'] == '20270115')

# Join each daily tick to the prevailing static record (asof join).
joined = otp.join_by_time([day, stat])

# Merge every matching AAPL contract into a single stream.
merged = otp.merge(
    [joined],
    symbols=otp.Symbols('US_OPTIONS_EOD_SAMPLE', pattern='AAPL %')
)

result = otp.run(
    merged,
    start=otp.dt(2025, 1, 3),
    end=otp.dt(2025, 1, 4),
    timezone='America/New_York'
)
result
```

## OPRA Daily Volume, Open Interest and Greeks for Selected Underlying and Expiry returned as a straddle configuration

OPRA Daily Volume, Open Interest and Greeks for Selected Underlying and Expiry returned as a straddle configuration.  
OPRA Daily data is retrieved from the ``US_OPTIONS_EOD`` database.  
Here the sample database ``US_OPTIONS_EOD_SAMPLE`` is used.  
Retrieves end-of-day pricing, Volume, Open Interest and Greeks (Implied Volatility, Delta, Gamma, Theta and Vega),
together with static data including Underlying Symbol, Strike Price, Expiration Date and Call/Put Indicator.  
Filtered for the ``AAPL`` underlying and expiration date 20270115.  
Data is displayed grouped by strike price, with Call and Put metrics associated with each strike.  
The daily ``DAY`` records are joined by time to the static ``STAT`` records with a lookback of up to 1 day (86400s).  
Call and Put metrics are created by testing ``CALL_PUT_IND`` ('P' for Put, 'C' for Call), using NaN for the
other side so that averaging by strike leaves a single row per strike carrying both the Call and Put values.

```{code-cell} ipython3

import onetick.py as otp

# Daily options data for all AAPL contracts.
day = otp.DataSource(db='US_OPTIONS_EOD_SAMPLE', tick_type='DAY')
day = day[['VOLUME', 'OPEN_INT', 'BID_CLOSE', 'ASK_CLOSE',
           'IMP_VOLATILITY', 'DELTA', 'GAMMA', 'THETA', 'VEGA']]

# Static (reference) data for the same contracts, looking back up to 1 day for the prevailing record.
stat = otp.DataSource(db='US_OPTIONS_EOD_SAMPLE', tick_type='STAT', back_to_first_tick=86400)
stat = stat[['UNDERLYING_SYMBOL', 'EXPIRATION_DATE', 'STRIKE_PRICE', 'CALL_PUT_IND']]

# Restrict the static records to the selected expiration.
stat = stat.where(stat['EXPIRATION_DATE'] == '20270115')

# Join each daily tick to the prevailing static record (asof join).
joined = otp.join_by_time([day, stat])

# Build Call ('C') and Put ('P') columns per metric; the opposite side is NaN so it is ignored by the average.
# (per-tick apply takes a single-argument lambda referencing the tick's fields)
joined['C_VOLUME'] = joined.apply(lambda t: t['VOLUME'] if t['CALL_PUT_IND'] == 'C' else otp.nan)
joined['P_VOLUME'] = joined.apply(lambda t: t['VOLUME'] if t['CALL_PUT_IND'] == 'P' else otp.nan)
joined['C_OPEN_INT'] = joined.apply(lambda t: t['OPEN_INT'] if t['CALL_PUT_IND'] == 'C' else otp.nan)
joined['P_OPEN_INT'] = joined.apply(lambda t: t['OPEN_INT'] if t['CALL_PUT_IND'] == 'P' else otp.nan)
joined['C_BID'] = joined.apply(lambda t: t['BID_CLOSE'] if t['CALL_PUT_IND'] == 'C' else otp.nan)
joined['P_BID'] = joined.apply(lambda t: t['BID_CLOSE'] if t['CALL_PUT_IND'] == 'P' else otp.nan)
joined['C_ASK'] = joined.apply(lambda t: t['ASK_CLOSE'] if t['CALL_PUT_IND'] == 'C' else otp.nan)
joined['P_ASK'] = joined.apply(lambda t: t['ASK_CLOSE'] if t['CALL_PUT_IND'] == 'P' else otp.nan)
joined['C_IV'] = joined.apply(lambda t: t['IMP_VOLATILITY'] if t['CALL_PUT_IND'] == 'C' else otp.nan)
joined['P_IV'] = joined.apply(lambda t: t['IMP_VOLATILITY'] if t['CALL_PUT_IND'] == 'P' else otp.nan)
joined['C_DELTA'] = joined.apply(lambda t: t['DELTA'] if t['CALL_PUT_IND'] == 'C' else otp.nan)
joined['P_DELTA'] = joined.apply(lambda t: t['DELTA'] if t['CALL_PUT_IND'] == 'P' else otp.nan)
joined['C_GAMMA'] = joined.apply(lambda t: t['GAMMA'] if t['CALL_PUT_IND'] == 'C' else otp.nan)
joined['P_GAMMA'] = joined.apply(lambda t: t['GAMMA'] if t['CALL_PUT_IND'] == 'P' else otp.nan)
joined['C_THETA'] = joined.apply(lambda t: t['THETA'] if t['CALL_PUT_IND'] == 'C' else otp.nan)
joined['P_THETA'] = joined.apply(lambda t: t['THETA'] if t['CALL_PUT_IND'] == 'P' else otp.nan)
joined['C_VEGA'] = joined.apply(lambda t: t['VEGA'] if t['CALL_PUT_IND'] == 'C' else otp.nan)
joined['P_VEGA'] = joined.apply(lambda t: t['VEGA'] if t['CALL_PUT_IND'] == 'P' else otp.nan)

# Average each Call and Put metric, grouped by Strike Price, to return a single straddle row per strike.
metrics = ['VOLUME', 'OPEN_INT', 'BID', 'ASK', 'IV', 'DELTA', 'GAMMA', 'THETA', 'VEGA']
agg_spec = {}
for m in metrics:
    agg_spec['C_' + m] = otp.agg.average('C_' + m)
    agg_spec['P_' + m] = otp.agg.average('P_' + m)

straddle = joined.agg(agg_spec, group_by=['STRIKE_PRICE'])

# Merge every matching AAPL contract into a single stream.
merged = otp.merge(
    [straddle],
    symbols=otp.Symbols('US_OPTIONS_EOD_SAMPLE', pattern='AAPL %')
)

result = otp.run(
    merged,
    start=otp.dt(2025, 1, 3),
    end=otp.dt(2025, 1, 4),
    timezone='America/New_York'
)
result
```

## OPRA Prevailing Pricing or Snapshot for the Selected Time, Underlying and Expiry

OPRA Trade data is retrieved from the ``US_OPTIONS`` database (sample database ``US_OPTIONS_SAMPLE``), together with
static data including Underlying Symbol, Strike Price, Expiration Date and Call/Put Indicator.  
Underlying equity pricing is retrieved from the US_COMP database (sample database ``US_COMP_SAMPLE``).  
Data is Filtered for the ``AAPL`` underlying (options via the ``AAPL %`` pattern, equity via SYMBOL_NAME ``AAPL``)
and for the expiration date 20270115.  
The option trades are joined by time to the static records and to the prevailing underlying equity trade.  
Because the data may have been published at different times before the target time, lookbacks are defined so
the prevailing values are returned.  
``MONEYNESS`` is computed as Strike Price minus the underlying price.

```python
import onetick.py as otp

# The snapshot time of interest.
snapshot_time = otp.dt(2025, 1, 3, 10)

# Prevailing options trades for all AAPL contracts, looking back up to 10 hours (36000s).
trd = otp.DataSource(db='US_OPTIONS_SAMPLE', tick_type='TRD', back_to_first_tick=36000)
trd = trd[['PRICE', 'SIZE']]

# Static (reference) data for the same contracts, looking back up to 1 day (86400s) for the prevailing record.
stat = otp.DataSource(db='US_OPTIONS_SAMPLE', tick_type='STAT', back_to_first_tick=86400)
stat = stat[['UNDERLYING_SYMBOL', 'EXPIRATION_DATE', 'STRIKE_PRICE', 'CALL_PUT_IND']]

# Restrict the static records to the selected expiration.
stat = stat.where(stat['EXPIRATION_DATE'] == '20270115')

# Join each prevailing option trade to the prevailing static record (asof join).
joined = otp.join_by_time([trd, stat])

# Keep only the last (prevailing) tick up to the snapshot time, per option contract.
joined = joined.last()

# Merge every matching AAPL option contract into a single stream.
options = otp.merge(
    [joined],
    symbols=otp.Symbols('US_OPTIONS_SAMPLE', pattern='AAPL %')
)

# Prevailing underlying equity trade for AAPL, looking back up to 10 hours (36000s).
underlying = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', back_to_first_tick=36000, symbols='AAPL')
underlying = underlying[['PRICE']]
underlying = underlying.rename({'PRICE': 'UNDERLYING_PRICE'})
underlying = underlying.last()

# Join the option snapshot to the prevailing underlying price (asof join).
result_src = otp.join_by_time([options, underlying])

# Moneyness is the strike price relative to the underlying price.
result_src['MONEYNESS'] = result_src['STRIKE_PRICE'] - result_src['UNDERLYING_PRICE']

# A zero-length window ending at the snapshot time returns the prevailing values as of that time.
result = otp.run(
    result_src,
    start=snapshot_time,
    end=snapshot_time,
    timezone='America/New_York'
)
result
```

## OPRA Prevailing or Snapshot Options Prices at a specified time, for a specified underlying

OPRA Trade data is retrieved from the ``US_OPTIONS`` database.  
Here the sample database ``US_OPTIONS_SAMPLE`` is used.  
Trades are joined by time to the static ``STAT`` records to obtain the Expiration Date, Strike Price and
Call/Put Indicator.  
The query returns the prevailing trade at the specified time; lookbacks are defined for
both the Trades (up to 10 hours) and the Static Data (up to 1 day) so the prevailing values up to the
specified time are returned.

```python
import onetick.py as otp

# The snapshot time of interest.
snapshot_time = otp.dt(2025, 1, 3, 10)

# Prevailing options trades for all AAPL contracts, looking back up to 10 hours (36000s).
trd = otp.DataSource(db='US_OPTIONS_SAMPLE', tick_type='TRD', back_to_first_tick=36000)
trd = trd[['PRICE', 'SIZE']]

# Static (reference) data for the same contracts, looking back up to 1 day (86400s) for the prevailing record.
stat = otp.DataSource(db='US_OPTIONS_SAMPLE', tick_type='STAT', back_to_first_tick=86400)
stat = stat[['UNDERLYING_SYMBOL', 'EXPIRATION_DATE', 'STRIKE_PRICE', 'CALL_PUT_IND']]

# Join each prevailing trade to the prevailing static record (asof join).
joined = otp.join_by_time([trd, stat])

# Keep only the last (prevailing) tick up to the snapshot time.
joined = joined.last()

# Merge every matching AAPL contract into a single stream.
merged = otp.merge(
    [joined],
    symbols=otp.Symbols('US_OPTIONS_SAMPLE', pattern='AAPL %')
)

# A zero-length window ending at the snapshot time returns the prevailing values as of that time.
result = otp.run(
    merged,
    start=snapshot_time,
    end=snapshot_time,
    timezone='America/New_York'
)
result
```

{.dataframe}
|     |                Time | PRICE | SIZE | UNDERLYING_SYMBOL | EXPIRATION_DATE | STRIKE_PRICE | CALL_PUT_IND |
|----:|--------------------:|------:|-----:|------------------:|----------------:|-------------:|-------------:|
|   0 | 2025-01-03 10:00:00 | 68.35 |   10 |              AAPL |        20250103 |        175.0 |            C |
|   1 | 2025-01-03 10:00:00 | 63.71 |   20 |              AAPL |        20250103 |        180.0 |            C |
|   2 | 2025-01-03 10:00:00 | 38.76 |    1 |              AAPL |        20250103 |        205.0 |            C |
|   3 | 2025-01-03 10:00:00 | 32.40 |    1 |              AAPL |        20250103 |        210.0 |            C |
|   4 | 2025-01-03 10:00:00 | 30.65 |    1 |              AAPL |        20250103 |        212.5 |            C |
| ... |                 ... |   ... |  ... |               ... |             ... |          ... |          ... |
| 633 | 2025-01-03 10:00:00 |  9.60 |   15 |              AAPL |        20270115 |        190.0 |            P |
| 634 | 2025-01-03 10:00:00 | 11.73 |    2 |              AAPL |        20270115 |        200.0 |            P |
| 635 | 2025-01-03 10:00:00 | 24.45 |    1 |              AAPL |        20270115 |        240.0 |            P |
| 636 | 2025-01-03 10:00:00 | 28.65 |   50 |              AAPL |        20270115 |        250.0 |            P |
| 637 | 2025-01-03 10:00:00 | 34.00 |    1 |              AAPL |        20270115 |        260.0 |            P |

638 rows x 7 columns

## OPRA Options Chain Symbol Universe Retrieval

Return the Options Chain of contracts for an OPRA underlying from the Symbol Universe.  
Filtering with ``US_OPTIONS Option`` selects the symbols that correspond to OPRA Options.  
Additionally filtering on ``UNDERLYING_SYMBOL`` equal to ``AAPL`` returns the option contracts for ``AAPL``.

```{code-cell} ipython3

import onetick.py as otp

# The Symbol Universe static records are stored in the SYMBOL_UNIVERSE database, STAT tick type.
# The SYMBOL_NAME 'US_OPTIONS Option' groups the OPRA option contracts.
data = otp.DataSource(db='SYMBOL_UNIVERSE', tick_type='STAT')

# Filter to the OPRA options universe for the AAPL underlying.
data = data.where(data['UNDERLYING_SYMBOL'] == 'AAPL')

# Select the descriptive fields of interest.
data = data[['DB_NAME', 'DB_SYMBOL', 'NAME', 'SEC_TYPE',
             'UNDERLYING_SYMBOL', 'CALL_PUT_IND', 'STRIKE_PRICE', 'EXPIRATION_DATE']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC',
    symbols='US_OPTIONS Option'
)
result
```

## OPRA Options Chain Symbol Universe Summary Retrieval

Return the Options Chain of contracts for an OPRA underlying from the Symbol Universe.  
Filtering with ``US_OPTIONS Option`` selects the symbols that correspond to OPRA Options.  
Additionally filtering on ``UNDERLYING_SYMBOL`` equal to ``AAPL`` returns the option contracts for ``AAPL``.

```{code-cell} ipython3

import onetick.py as otp

# The Symbol Universe static records are stored in the SYMBOL_UNIVERSE database, STAT tick type.
# The SYMBOL_NAME 'US_OPTIONS Option' groups the OPRA option contracts.
data = otp.DataSource(db='SYMBOL_UNIVERSE', tick_type='STAT')

# Filter to the OPRA options universe for the AAPL underlying.
data = data.where(data['UNDERLYING_SYMBOL'] == 'AAPL')

# Select the descriptive fields of interest.
data = data[['DB_NAME', 'DB_SYMBOL', 'NAME', 'SEC_TYPE',
             'UNDERLYING_SYMBOL', 'CALL_PUT_IND', 'STRIKE_PRICE', 'EXPIRATION_DATE']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC',
    symbols='US_OPTIONS Option'
)
result
```

## CME Options Symbol Universe Retrieval

Return the number of Options contracts for ``CME`` from the Symbol Universe.  
Filtering with ``CME Option`` selects the symbols that correspond to CME Options.  
``CME`` populates ``UNDERLYING_SEC_TYPE``, allowing Products to be grouped.

```{code-cell} ipython3

import onetick.py as otp

# The Symbol Universe static records are stored in the SYMBOL_UNIVERSE database, STAT tick type.
# The SYMBOL_NAME 'CME Option' groups the CME option contracts.
data = otp.DataSource(db='SYMBOL_UNIVERSE', tick_type='STAT')

# Count the contracts, grouped by database, security type, underlying security type and product code.
summary = data.agg(
    {'CONTRACT_COUNT': otp.agg.count()},
    group_by=['DB_NAME', 'SEC_TYPE', 'UNDERLYING_SEC_TYPE', 'PRODUCT_CODE']
)

# Return first 1000 rows
summary = summary.limit(1000)

result = otp.run(
    summary,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC',
    symbols='CME Option'
)
result
```

## CME Treasury Options Symbol Universe Retrieval

Return Interest Rate Options contracts for ``CME`` from the Symbol Universe.  
Filtering with ``CME Option`` selects the symbols that correspond to CME Options.  
Filtering with ``UNDERLYING_SEC_TYPE`` set to ``Interest Rate`` restricts the results to Treasury Options.

```{code-cell} ipython3

import onetick.py as otp

# The Symbol Universe static records are stored in the SYMBOL_UNIVERSE database, STAT tick type.
# The SYMBOL_NAME 'CME Option' groups the CME option contracts.
data = otp.DataSource(db='SYMBOL_UNIVERSE', tick_type='STAT')

# Filter to Interest Rate (Treasury) options.
data = data.where(data['UNDERLYING_SEC_TYPE'] == 'Interest Rate')

# Select the descriptive fields of interest.
data = data[['DB_NAME', 'DB_SYMBOL', 'NAME', 'SEC_TYPE', 'UNDERLYING_SEC_TYPE',
             'PRODUCT_CODE', 'CALL_PUT_IND', 'STRIKE_PRICE', 'EXPIRATION_DATE']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC',
    symbols='CME Option'
)
result
```

## CME Options Chain Symbol Universe Retrieval

Return the Options Chain of contracts for a CME Product from the Symbol Universe.  
Filtering with ``CME Option`` selects the symbols that correspond to CME Options.  
Additionally filtering on ``PRODUCT_CODE`` equal to ``LO``, the CME Product Code for Crude Oil.

```{code-cell} ipython3

import onetick.py as otp

# The Symbol Universe static records are stored in the SYMBOL_UNIVERSE database, STAT tick type.
# The SYMBOL_NAME 'CME Option' groups the CME option contracts.
data = otp.DataSource(db='SYMBOL_UNIVERSE', tick_type='STAT')

# Filter to the CME Crude Oil product (PRODUCT_CODE 'LO').
data = data.where(data['PRODUCT_CODE'] == 'LO')

# Select the descriptive fields of interest.
data = data[['DB_NAME', 'DB_SYMBOL', 'NAME', 'SEC_TYPE', 'UNDERLYING_SEC_TYPE',
             'PRODUCT_CODE', 'CALL_PUT_IND', 'STRIKE_PRICE', 'EXPIRATION_DATE']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC',
    symbols='CME Option'
)
result
```

## Query Options Volume Open Interest And Greeks Analysis

Options Volume, Open Interest, and Greeks Analysis by Expiration and Call/Put.

```{code-cell} ipython3

import onetick.py as otp

# Define the time range
start_time = otp.dt(2025, 1, 20)
end_time = otp.dt(2025, 1, 28)

# Create a DataSource for all AAPL option contracts
options_day = otp.DataSource(
    db='US_OPTIONS_EOD_SAMPLE',
    tick_type='DAY'
)

# Merge all AAPL option contracts into a single stream
merged_options = otp.merge(
    [options_day],
    symbols=otp.Symbols('US_OPTIONS_EOD_SAMPLE', pattern='AAPL%')
)

# Aggregate metrics across all contracts
agg_result = merged_options.agg(
    {
        'TOTAL_OPEN_INTEREST': otp.agg.sum('OPEN_INT'),
        'TOTAL_VOLUME': otp.agg.sum('VOLUME'),
        'AVG_UNDERLYING_PRICE': otp.agg.average('UNDERLYING_PRICE'),
        'AVG_IMPLIED_VOL': otp.agg.average('IMP_VOLATILITY'),
        'AVG_DELTA': otp.agg.average('DELTA'),
        'AVG_GAMMA': otp.agg.average('GAMMA'),
        'AVG_THETA': otp.agg.average('THETA'),
        'AVG_VEGA': otp.agg.average('VEGA'),
        'CONTRACT_COUNT': otp.agg.count()
    }
)

# Run the query
df = otp.run(
    agg_result,
    start=start_time,
    end=end_time,
    timezone='UTC'
)

# The result is a single DataFrame with one row
df
```
