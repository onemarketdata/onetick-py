---
file_format: mystnb
---

# Futures

This section contains 10 examples for Futures using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Calculates Point in Time Trade Snapshot for Futures Product (Futures Chain)

A specific point in time is selected (e.g. 2024-01-03 12:30:00 Europe/London).  
All futures symbols for the
product are retrieved with a symbol pattern (e.g. ``BRN____`` for Brent Crude on ICE Europe Commodities).  
A lookback of up to 1 day (86400s) returns the prevailing trade before the selected time, per contract.

```{code-cell} ipython3

import onetick.py as otp

# The snapshot time of interest.
snapshot_time = otp.dt(2024, 1, 3, 12, 30)

# Trade data for the ICE Europe Brent Crude futures chain, looking back up to 1 day for the prevailing trade.
trd = otp.DataSource(db='ICE_EU_COM_SAMPLE', tick_type='TRD', back_to_first_tick=86400)

# Keep only the last (prevailing) tick up to the snapshot time, per contract.
trd = trd.last()

# Merge the prevailing trade for every matching BRN futures contract into a single stream.
merged = otp.merge(
    [trd],
    symbols=otp.Symbols('ICE_EU_COM_SAMPLE', pattern='BRN____', for_tick_type='TRD')
)

# A zero-length window ending at the snapshot time returns the prevailing values as of that time.
result = otp.run(
    merged,
    start=snapshot_time,
    end=snapshot_time,
    timezone='Europe/London'
)
result
```

## Return the Futures Chain of contracts for a NYMEX Product from the Symbol Universe

Filtering with ``NYMEX Future`` selects the symbols that correspond to NYMEX Futures.  
Additionally filtering on ``PRODUCT_CODE`` equal to ``CL``, the NYMEX Product Code for Crude Oil.

```{code-cell} ipython3

import onetick.py as otp

# The Symbol Universe static records are stored in the SYMBOL_UNIVERSE database, STAT tick type.
# The SYMBOL_NAME 'NYMEX Future' groups the NYMEX futures contracts.
data = otp.DataSource(db='SYMBOL_UNIVERSE', tick_type='STAT')

# Filter to the NYMEX Crude Oil product (PRODUCT_CODE 'CL').
data = data.where(data['PRODUCT_CODE'] == 'CL')

# Select the descriptive fields of interest.
data = data[['DB_NAME', 'DB_SYMBOL', 'BSYM', 'NAME', 'SEC_TYPE',
             'UNDERLYING_SEC_TYPE', 'PRODUCT_CODE', 'EXPIRATION_DATE']]

# Return first 1000 rows
data = data.limit(1000)

result = otp.run(
    data,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC',
    symbols='NYMEX Future'
)
result
```

## Return the first 1000 Futures from the Symbol Universe

The Symbol Universe groups contracts by a ``SYMBOL_NAME`` marker.  
The ``% Future`` markers cover every database that includes Futures.  
A one-day time window returns the futures contracts active in that period.

```{code-cell} ipython3

import onetick.py as otp

# The Symbol Universe static records are stored in the SYMBOL_UNIVERSE database, STAT tick type.
# Query each futures marker symbol and merge the results into one stream.
data = otp.DataSource(db='SYMBOL_UNIVERSE', tick_type='STAT')
data = data[['DB_NAME', 'DB_SYMBOL', 'BSYM', 'NAME', 'PRODUCT_CODE',
             'SEC_TYPE', 'UNDERLYING_SEC_TYPE', 'EXPIRATION_DATE']]

# Merge across all marker symbols that end in ' Future' (e.g. 'NYMEX Future', 'CME Future', ...).
merged = otp.merge(
    [data],
    symbols=otp.Symbols('SYMBOL_UNIVERSE', pattern='% Future', for_tick_type='STAT')
)

# Return first 1000 rows
merged = merged.limit(1000)

result = otp.run(
    merged,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC'
)
result
```

## Return the number of Futures contracts for NYMEX from the Symbol Universe

Filtering with ``NYMEX Future`` selects the symbols that correspond to NYMEX Futures.  
NYMEX populates ``UNDERLYING_SEC_TYPE``, allowing Products to be grouped.

```{code-cell} ipython3

import onetick.py as otp

# The Symbol Universe static records are stored in the SYMBOL_UNIVERSE database, STAT tick type.
# The SYMBOL_NAME 'NYMEX Future' groups the NYMEX futures contracts.
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
    symbols='NYMEX Future'
)
result
```

## Trades for Product / Futures Chain

Return the first 1000 trades for Crude Oil Futures contracts (Futures Chain) trading on ``NYMEX``,
product code ``CL``.  
Futures symbols have the structure ``[Product Code]\[Expiry Month & Year]``, e.g. ``CL\N26``.  
The symbol pattern ``CL____`` selects the futures chain: the product code ``CL`` followed by the backslash and the
three-character expiry code (single-character wildcards each match one character, including the backslash).

```{code-cell} ipython3

import onetick.py as otp

# Trade data for the NYMEX Crude Oil futures chain.
trd = otp.DataSource(db='NYMEX', tick_type='TRD')

# Merge every matching CL futures contract into a single stream.
merged = otp.merge(
    [trd],
    symbols=otp.Symbols('NYMEX', pattern='CL____', for_tick_type='TRD')
)

# Return first 1000 rows
merged = merged.limit(1000)

result = otp.run(
    merged,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC'
)
result
```

## Trades for Product / Futures Spreads Chain

Return the first 1000 trades for Crude Oil Futures Spreads contracts (Futures Spreads Chain) trading on
``NYMEX``, product code ``CL``.  
Futures Spread symbols have the structure
``[Product Code]\[Expiry Month & Year]\[Expiry Month & Year]``, e.g. ``CL\N26\Z26``.  
The symbol pattern 'CL________' selects the spreads chain: the product code CL followed by eight
characters (both backslashes and the two three-character expiry codes).

```{code-cell} ipython3

import onetick.py as otp

# Trade data for the NYMEX Crude Oil futures spreads chain.
trd = otp.DataSource(db='NYMEX', tick_type='TRD')

# Merge every matching CL spread contract into a single stream.
merged = otp.merge(
    [trd],
    symbols=otp.Symbols('NYMEX', pattern='CL________', for_tick_type='TRD')
)

# Return first 1000 rows
merged = merged.limit(1000)

result = otp.run(
    merged,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC'
)
result
```

## Trades for Product for both Futures and Spreads

Return the first 1000 trades for Crude Oil contracts, whether Futures or Spreads, trading on ``NYMEX``,
product code ``CL``.  
Futures symbols have the structure ``[Product Code]\[Expiry Month & Year]`` (e.g. ``CL\N26``),
and Futures Spread symbols the structure ``[Product Code]\[Expiry Month & Year]\[Expiry Month & Year]``
(e.g. ``CL\N26\Z26``).  
Both chains are selected by their symbol patterns and merged together.

```{code-cell} ipython3

import onetick.py as otp

# Trade data for the NYMEX Crude Oil contracts.
trd = otp.DataSource(db='NYMEX', tick_type='TRD')

# Select the outright futures chain ('CL____') and the spreads chain ('CL________').
futures = otp.Symbols('NYMEX', pattern='CL____', for_tick_type='TRD')
spreads = otp.Symbols('NYMEX', pattern='CL________', for_tick_type='TRD')

# Merge trades from both chains into a single stream.
merged = otp.merge(
    [trd],
    symbols=otp.merge([futures, spreads])
)

# Return first 1000 rows
merged = merged.limit(1000)

result = otp.run(
    merged,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC'
)
result
```

## Volume and Open Interest (OI) for Product / Futures Chain

Return the Volume and Open Interest for the first 1000 Crude Oil Futures contracts (Futures Chain)
trading on ``NYMEX``, product code ``CL``.  
The symbol pattern ``CL____`` selects the futures chain.  
``UPDATE_TYPE`` is filtered to ``Summary`` to return the final daily combination of both Volume and Open
Interest (other records carry only Volume or only Open Interest updates).

```{code-cell} ipython3

import onetick.py as otp

# Daily records for the NYMEX Crude Oil futures chain.
day = otp.DataSource(db='NYMEX_DAILY', tick_type='DAY')

# Keep only the daily Summary records that carry both Volume and Open Interest.
day = day.where(day['UPDATE_TYPE'] == 'Summary')

# Merge every matching CL futures contract into a single stream.
merged = otp.merge(
    [day],
    symbols=otp.Symbols('NYMEX_DAILY', pattern='CL____', for_tick_type='DAY')
)

# Return first 1000 rows
merged = merged.limit(1000)

result = otp.run(
    merged,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC'
)
result
```

## Volume and Open Interest (OI) by Expiry for Product / Futures Chain

Return the Volume and Open Interest for the Crude Oil Futures contracts (Futures Chain) trading on ``NYMEX``,
product code ``CL``, together with the Expiration Date.  
The symbol pattern ``CL____`` selects the futures chain.  
``UPDATE_TYPE`` is filtered to ``Summary`` to return the final daily combination of both Volume and Open Interest.  
The daily ``DAY`` records are joined by time to the static ``STAT`` records (which carry the Expiration Date),
with a lookback of up to 1 day (86400s) so the prevailing static record is picked up.  
Results are ordered by Expiration Date.

```{code-cell} ipython3

import onetick.py as otp

# Daily records for the NYMEX Crude Oil futures chain.
day = otp.DataSource(db='NYMEX_DAILY', tick_type='DAY')
day = day.where(day['UPDATE_TYPE'] == 'Summary')
day = day[['VOLUME', 'OPEN_INT']]

# Static (reference) data carrying the Expiration Date, looking back up to 1 day for the prevailing record.
stat = otp.DataSource(db='NYMEX_DAILY', tick_type='STAT', back_to_first_tick=86400)
stat = stat[['EXPIRATION_DATE']]

# Join each daily tick to the prevailing static record (asof join).
joined = otp.join_by_time([day, stat])

# Merge every matching CL futures contract into a single stream.
merged = otp.merge(
    [joined],
    symbols=otp.Symbols('NYMEX_DAILY', pattern='CL____', for_tick_type='DAY')
)

# Order the contracts by Expiration Date.
merged = merged.sort('EXPIRATION_DATE')

result = otp.run(
    merged,
    start=otp.dt(2026, 6, 11),
    end=otp.dt(2026, 6, 12),
    timezone='UTC'
)
result
```

## Getting Daily Trade Bars for Product

Query CME E-mini S&P 500 futures contracts daily bar data matching the pattern ``ES____`` for a single day.

```{code-cell} ipython3

import onetick.py as otp

# Define the time range
start = otp.dt(2024, 1, 3)
end = otp.dt(2024, 1, 4)

# Get all symbols matching 'ES____' (ES + 4 wildcard characters)
symbols = otp.Symbols(
    db='CME_SAMPLE_DAILY',
    pattern='ES____',
    for_tick_type='DAY'
)

# Define the data source for the DAY tick type
data = otp.DataSource(db='CME_SAMPLE_DAILY', tick_type='DAY')

# merging all symbols into a single flow
data = otp.merge([data], symbols=symbols, identify_input_ts=True)

# Run the query
result = otp.run(
    data,
    start=start,
    end=end,
    timezone='America/New_York'
)

result
```
