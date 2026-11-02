---
file_format: mystnb
---

# Composites

This section contains examples for Composites using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Daily Composite Bars

Returns pre-calculated Daily OHLCV (Open, High, Low, Close, Volume) data
for a specified symbol and date range for a Composite.  
Multiple rows are returned per day: one per `EXCHANGE`/`VENUE_ID`
(`US_COMP` uses `EXCHANGE`, other composites use `VENUE_ID`),
plus the primary venue as `'PRIM'` and the composite itself as an empty string.

US Composite Trade Daily Bars Including Volume Split by Trade Type.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')
data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Total Volume Per Venue Across Symbols

Calculating the Volume Traded across all symbols for each venue within the Composite. e.g. ``US_COMP``.  
The ``DAY`` summary table includes the daily Volume rollup,
making it unnecessary to calculate across the Trade table ``TRD``.  
The venue is stored in the field ``EXCHANGE`` for ``US_COMP``, and ``QUOTE_VENUE`` for other composites.  
The composite for all venues is typically added as empty quotes e.g. ``''``.  
The primary venue is identified as ``PRIM``.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve DAY Records from US_COMP_DAILY
data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')

# Limit Fields
data = data[['EXCHANGE', 'VOLUME']]

# Merge across all symbols
data = otp.merge([data], symbols=otp.Symbols(db='US_COMP_SAMPLE_DAILY'), identify_input_ts=True)

# Aggregate: count number of quotes per venue
data = data.agg({'VOLUME': otp.agg.sum('VOLUME')}, group_by=['EXCHANGE'])

# Filter out the Composite
data = data.where(data['EXCHANGE'] != '')

# Filter out the Primary
data = data.where(data['EXCHANGE'] != 'PRIM')

# Run query across the time range.
result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York')
result
```

## Trade Count Per Venue

Calculating the trade count for each venue within the Composite.  e.g. ``CA_COMP``.  
The quotes are stored in the ``TRD`` table.  
The venue is stored in the field ``TRADE_VENUE``, except for ``US_COMP`` which uses ``EXCHANGE``.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve Trades from CA_COMP
data = otp.DataSource(db='CA_COMP_SAMPLE', tick_type='TRD')

# Aggregate: count number of trades per venue
data = data.agg({'TRADE_COUNT': otp.agg.count()}, group_by=['TRADE_VENUE'])

# Define Output fields
data = data[['TRADE_VENUE', 'TRADE_COUNT']]

# Run query for selected symbol (for TD) and time range.
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/Toronto',
                 symbols='TD')
result
```

## Quote Count Per Venue

Calculating the quote count for each venue within the Composite. e.g. ``CA_COMP``.  
The quotes are stored in the ``QTE`` table.  
The venue is stored in the field ``QUOTE_VENUE``, except for ``US_COMP`` which uses ``EXCHANGE``.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve Quotes from CA_COMP
data = otp.DataSource(db='CA_COMP_SAMPLE', tick_type='QTE')

# Aggregate: count number of quotes per venue
data = data.agg({'QUOTE_COUNT': otp.agg.count()}, group_by=['QUOTE_VENUE'])

# Define Output fields
data = data[['QUOTE_VENUE', 'QUOTE_COUNT']]

# Run query for selected symbol (for TD) and time range.
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/Toronto',
                 symbols='TD')
result
```

## EBBO Custom Calculation

Calculate a Custom EBBO / NBBO based on Selected Venues using the European Composite Dataset ``EU_COMP``.  
The European Composite includes quotes from all European Venues, plus a consolidated NBBO.  
A custom NBBO can be calculated by filtering consolidated quotes on venue using the ``QUOTE_VENUE`` field
and currency using the ``CURRENCY`` field.  
The {meth}`~onetick.py.Source.virtual_ob` method is used in association with the
{meth}`~onetick.py.Source.ob_snapshot_wide` method to construct the new consolidated book.  
Using parameter `max_levels=1` to limit the resulting book to Top of Book (ToB) to produce the NBBO.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve Quotes from EU_COMP
data = otp.DataSource(db='EU_COMP_SAMPLE', tick_type='QTE')

# Filter on Continuous Trading Periods
data = data.where(data['OMD_STATUS'] == 'T')

# Filter on Required Currency
data = data.where(data['CURRENCY'] == 'GBX')

# Filter on Selected Venues
data = data.where(data['QUOTE_VENUE'].isin('AQXE', 'BATE', 'CHIX', 'EQTC', 'TRQX', 'XLON'))

# Create a order book data format based on the quotes from each venue
data = data.virtual_ob(['QUOTE_VENUE'])

# Rebuild the NBBO from the order book data
data = data.ob_snapshot_wide(running=True, max_levels=1)

# Add Spread
data['SPREAD'] = data['ASK_PRICE'] - data['BID_PRICE']

# Filter out events that occur at the same timestamp, keeping the last event per timestamp
data['NEXT_TS'] = data['TIMESTAMP'][+1]
data = data.where(data['NEXT_TS'] > data['TIMESTAMP'])

# Define Output fields
data = data[['BID_PRICE', 'BID_SIZE', 'ASK_PRICE', 'ASK_SIZE', 'SPREAD']]

# Return first 100 Rows
data = data.limit(100)

# Run query for selected ISIN (for Vodafone) and time range.
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8, 0),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='Europe/London',
                 symbols='GB00BH4HKS39')
result
```

## CBBO Custom Calculation

Calculate a Custom CBBO / NBBO based on Selected Venues using the Canadian Composite Dataset ``CA_COMP``.  
The Canadian Composite includes quotes from all Canadian Venues, plus a consolidated NBBO.  
A custom NBBO can be calculated by filtering on venue using the ``QUOTE_VENUE`` field.  
The {meth}`~onetick.py.Source.virtual_ob` method is used in association with the
{meth}`~onetick.py.Source.ob_snapshot_wide` method to construct the new consolidated book.  
Using parameter `max_levels=1` to limit the resulting book to Top of Book (ToB) to produce the NBBO.

```{code-cell} ipython3

import onetick.py as otp

# Retrieve Quotes from CA_COMP
data = otp.DataSource(db='CA_COMP_SAMPLE', tick_type='QTE')

# Filter on Continuous Trading Periods
data = data.where(data['OMD_STATUS'] == 'T')

# Filter on Selected Venues
data = data.where(
    data['QUOTE_VENUE'].isin('CHIC', 'CSE2', 'LYNX', 'NEOE', 'NEON', 'OMGA', 'PURE', 'XATS', 'XATX', 'XCX2', 'XTSE')
)

# Create a order book data format based on the quotes from each venue
data = data.virtual_ob(['QUOTE_VENUE'])

# Rebuild the NBBO from the order book data
data = data.ob_snapshot_wide(running=True, max_levels=1)

# Add Spread
data['SPREAD'] = data['ASK_PRICE'] - data['BID_PRICE']

# Filter out events that occur at the same timestamp, keeping the last event per timestamp
data['NEXT_TS'] = data['TIMESTAMP'][+1]
data = data.where(data['NEXT_TS'] > data['TIMESTAMP'])

# Define Output fields
data = data[['BID_PRICE', 'BID_SIZE', 'ASK_PRICE', 'ASK_SIZE', 'SPREAD']]

# Return first 100 Rows
data = data.limit(100)

# Run query for selected ISIN (for Vodafone) and time range.
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/Toronto',
                 symbols='TD')
result
```

## Trade Volume to NBBO Calculation

Calculate trade volume relative to NBBO levels for each trade.  
Joins trades (``TRD``) with prevailing ``NBBO`` quotes and classifies each trade by NBBO level.  
Classifies each trade volume by NBBO level (``AT_MID``, ``INSIDE_NBBO``, ``AT_NBBO``, ``OUTSIDE_NBBO``).  
Aggregates total volume by exchange and symbol for each NBBO classification.  
Calculate trade volume relative to NBBO levels for each trade.

* Step 1: For each trade, retrieve the prevailing NBBO at that moment
* Step 2: Classify each trade volume by NBBO level (``AT_MID``, ``INSIDE_NBBO``, ``AT_NBBO``, ``OUTSIDE_NBBO``)
* Step 3: Aggregate total volume by exchange for each NBBO classification

```{code-cell} ipython3

import onetick.py as otp

# Retrieve trade ticks with PRICE and SIZE fields
trades = otp.DataSource(db='US_COMP', tick_type='TRD')
trades = trades[['PRICE', 'SIZE', 'EXCHANGE']]

# Retrieve prevailing NBBO quotes with BID_PRICE and ASK_PRICE fields
nbbo = otp.DataSource(db='US_COMP', tick_type='NBBO', back_to_first_tick=86400)
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE']]

# Join trades with prevailing NBBO at trade time
joined = otp.join_by_time([trades, nbbo])

# Calculate mid price
joined['MID_PRICE'] = (joined['BID_PRICE'] + joined['ASK_PRICE']) / 2

# Classify trade volume by NBBO level using conditional logic (boolean * size = size or 0)
joined['VOLUME_AT_MID'] = (joined['PRICE'] == joined['MID_PRICE']) * joined['SIZE']

joined['VOLUME_INSIDE_NBBO'] = (
    (joined['PRICE'] > joined['BID_PRICE']) & (joined['PRICE'] < joined['ASK_PRICE'])
) * joined['SIZE']

joined['VOLUME_AT_NBBO'] = (
    (joined['PRICE'] == joined['BID_PRICE']) | (joined['PRICE'] == joined['ASK_PRICE'])
) * joined['SIZE']

joined['VOLUME_OUTSIDE_NBBO'] = (
    (joined['PRICE'] < joined['BID_PRICE']) | (joined['PRICE'] > joined['ASK_PRICE'])
) * joined['SIZE']

# Aggregate by exchange and symbol
data = joined.agg({
    'VOLUME_AT_MID': otp.agg.sum('VOLUME_AT_MID'),
    'VOLUME_INSIDE_NBBO': otp.agg.sum('VOLUME_INSIDE_NBBO'),
    'VOLUME_AT_NBBO': otp.agg.sum('VOLUME_AT_NBBO'),
    'VOLUME_OUTSIDE_NBBO': otp.agg.sum('VOLUME_OUTSIDE_NBBO'),
    'TRADE_COUNT': otp.agg.count()
}, group_by=['EXCHANGE'])

result = otp.run(
    data,
    symbols=['CSCO'],
    start=otp.dt(2024, 1, 3),
    end=otp.dt(2024, 1, 4),
    timezone='America/New_York'
)
result
```

## US NBBO with Odd Lot Retrieval

The US Composite provides data from the US Consolidated Tape, known as the SIP.  
Historically Quotes and NBBO have been provided based on round lots, and Odd lot quotes have not been distributed.  
Regulation has since been updated so that Odd lot quotes are additionally distributed,
adding the Best Odd Lot Order (BOLO).  
OneTick combines the BOLO with the NBBO to create `NBBO_COMP`, which is available from June 2026 onwards.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP', tick_type='NBBO_COMP')
data = data.limit(1000)
result = otp.run(data,
                 start=otp.dt(2026, 7, 23, 9, 30),
                 end=otp.dt(2026, 7, 23, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## US Quotes with Odd Lot Retrieval

The US Composite provides data from the US Consolidated Tape, known as the SIP.  
Historically Quotes and NBBO have been provided based on round lots, and Odd lot quotes have not been distributed.  
Regulation has since been updated so that Odd lot quotes are additionally distributed,
adding the Best Odd Lot Order (BOLO).  
OneTick combines the BOLO with the exchange quotes to create `QTE_COMP`, which is available from June 2026 onwards.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP', tick_type='QTE_COMP')
data = data.limit(1000)
result = otp.run(data,
                 start=otp.dt(2026, 7, 23, 9, 30),
                 end=otp.dt(2026, 7, 23, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Retrieving the US Exchange NBBO with and without Odd Lots (BOLO)

The NBBO with Odd Lots (`NBBO_COMP`) is joined to the NBBO without Odd Lots.  
This returns the two sets of quotes: the wider spread without odd lots and the narrower spread with odd lots.

```{code-cell} ipython3

import onetick.py as otp

# NBBO without Odd Lots
nbbo = otp.DataSource(db='US_COMP', tick_type='NBBO')
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE', 'BID_SIZE_TOTAL', 'ASK_SIZE_TOTAL']]

# NBBO with Odd Lots (BOLO) - renamed with a _COMP suffix
comp = otp.DataSource(db='US_COMP', tick_type='NBBO_COMP')
comp = comp[['BID_PRICE', 'ASK_PRICE', 'BID_SIZE_TOTAL', 'ASK_SIZE_TOTAL']]
comp = comp.rename(columns={'BID_PRICE': 'BID_PRICE_COMP',
                            'ASK_PRICE': 'ASK_PRICE_COMP',
                            'BID_SIZE_TOTAL': 'BID_SIZE_TOTAL_COMP',
                            'ASK_SIZE_TOTAL': 'ASK_SIZE_TOTAL_COMP'})

# As-of join: keep every NBBO tick, attaching the prevailing NBBO_COMP quote
data = otp.join_by_time([nbbo, comp])
data = data.limit(1000)
result = otp.run(data,
                 start=otp.dt(2026, 7, 23, 9, 30),
                 end=otp.dt(2026, 7, 23, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

### Retrieving the US Exchange NBBO with and without Odd Lots calculating Skews and Liquidity

The NBBO with Odd Lots (`NBBO_COMP`) is joined to the NBBO without Odd Lots, returning the two sets of
quotes: the wider spread without odd lots and the narrower spread with odd lots.  
These are compared to produce the Bid and Ask Skews (`BID_SKEW`, `ASK_SKEW`) and Percentage Skew
(`PCNT_BID_SKEW`, `PCNT_ASK_SKEW`).  
Additional liquidity visible in the odd lot quote is also calculated by comparing the Bid and Ask Sizes
between the two sets of quotes.  
This is not the full additional liquidity, as there may be price levels between the two sets of quotes.

```{code-cell} ipython3

import onetick.py as otp

# NBBO without Odd Lots
nbbo = otp.DataSource(db='US_COMP', tick_type='NBBO')
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE', 'BID_SIZE_TOTAL', 'ASK_SIZE_TOTAL']]

# NBBO with Odd Lots (BOLO) - renamed with a _COMP suffix
comp = otp.DataSource(db='US_COMP', tick_type='NBBO_COMP')
comp = comp[['BID_PRICE', 'ASK_PRICE', 'BID_SIZE_TOTAL', 'ASK_SIZE_TOTAL']]
comp = comp.rename(columns={'BID_PRICE': 'BID_PRICE_COMP',
                            'ASK_PRICE': 'ASK_PRICE_COMP',
                            'BID_SIZE_TOTAL': 'BID_SIZE_TOTAL_COMP',
                            'ASK_SIZE_TOTAL': 'ASK_SIZE_TOTAL_COMP'})

# As-of join: keep every NBBO tick, attaching the prevailing NBBO_COMP quote
data = otp.join_by_time([nbbo, comp])

# Skews - odd lot book is narrower, so compare against the round lot NBBO
data['BID_SKEW'] = data['BID_PRICE_COMP'] - data['BID_PRICE']
data['ASK_SKEW'] = data['ASK_PRICE'] - data['ASK_PRICE_COMP']
data['PCNT_BID_SKEW'] = 100 * (data['BID_PRICE_COMP'] - data['BID_PRICE']) / data['BID_PRICE']
data['PCNT_ASK_SKEW'] = 100 * (data['ASK_PRICE'] - data['ASK_PRICE_COMP']) / data['ASK_PRICE']

# Additional visible liquidity from the odd lot quote.
# If the price levels match, the addition is the size difference, otherwise it is the full odd lot size.
data['BID_SIZE_ADDITION'] = data.apply(
    lambda r: r['BID_SIZE_TOTAL_COMP'] - r['BID_SIZE_TOTAL']
    if r['BID_PRICE_COMP'] == r['BID_PRICE']
    else r['BID_SIZE_TOTAL_COMP']
)
data['ASK_SIZE_ADDITION'] = data.apply(
    lambda r: r['ASK_SIZE_TOTAL_COMP'] - r['ASK_SIZE_TOTAL']
    if r['ASK_PRICE_COMP'] == r['ASK_PRICE']
    else r['ASK_SIZE_TOTAL_COMP']
)

data = data.limit(1000)
result = otp.run(data,
                 start=otp.dt(2026, 7, 23, 9, 30),
                 end=otp.dt(2026, 7, 23, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Calculating 1 Minute TWAP Bars for Spreads Skews and added liquidity comparing NBBOs with and without Odd Lots

The NBBO with Odd Lots (`NBBO_COMP`) is joined to the NBBO without Odd Lots.  
Spreads, Skews and added visible Liquidity are calculated.  
These are then calculated as 1 minute Time weighted averages using {func}`~onetick.py.agg.tw_average` across the period.

```{code-cell} ipython3

import onetick.py as otp

# NBBO without Odd Lots
nbbo = otp.DataSource(db='US_COMP', tick_type='NBBO')
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE', 'BID_SIZE_TOTAL', 'ASK_SIZE_TOTAL']]

# NBBO with Odd Lots (BOLO) - renamed with a _COMP suffix
comp = otp.DataSource(db='US_COMP', tick_type='NBBO_COMP')
comp = comp[['BID_PRICE', 'ASK_PRICE', 'BID_SIZE_TOTAL', 'ASK_SIZE_TOTAL']]
comp = comp.rename(columns={'BID_PRICE': 'BID_PRICE_COMP',
                            'ASK_PRICE': 'ASK_PRICE_COMP',
                            'BID_SIZE_TOTAL': 'BID_SIZE_TOTAL_COMP',
                            'ASK_SIZE_TOTAL': 'ASK_SIZE_TOTAL_COMP'})

# As-of join: keep every NBBO tick, attaching the prevailing NBBO_COMP quote
data = otp.join_by_time([nbbo, comp])

# Spreads with and without odd lots
data['SPREAD'] = data['ASK_PRICE'] - data['BID_PRICE']
data['SPREAD_COMP'] = data['ASK_PRICE_COMP'] - data['BID_PRICE_COMP']

# Percentage skews
data['PCNT_BID_SKEW'] = 100 * (data['BID_PRICE_COMP'] - data['BID_PRICE']) / data['BID_PRICE']
data['PCNT_ASK_SKEW'] = 100 * (data['ASK_PRICE'] - data['ASK_PRICE_COMP']) / data['ASK_PRICE']

# Additional visible liquidity from the odd lot quote
data['BID_SIZE_ADDITION'] = data.apply(
    lambda r: r['BID_SIZE_TOTAL_COMP'] - r['BID_SIZE_TOTAL']
    if r['BID_PRICE_COMP'] == r['BID_PRICE']
    else r['BID_SIZE_TOTAL_COMP']
)
data['ASK_SIZE_ADDITION'] = data.apply(
    lambda r: r['ASK_SIZE_TOTAL_COMP'] - r['ASK_SIZE_TOTAL']
    if r['ASK_PRICE_COMP'] == r['ASK_PRICE']
    else r['ASK_SIZE_TOTAL_COMP']
)

# 1 minute time weighted averages
data = data.agg({'TWA_SPREAD': otp.agg.tw_average('SPREAD'),
                 'TWA_SPREAD_COMP': otp.agg.tw_average('SPREAD_COMP'),
                 'TWA_PCNT_BID_SKEW': otp.agg.tw_average('PCNT_BID_SKEW'),
                 'TWA_PCNT_ASK_SKEW': otp.agg.tw_average('PCNT_ASK_SKEW'),
                 'TWA_BID_SIZE_ADDITION': otp.agg.tw_average('BID_SIZE_ADDITION'),
                 'TWA_ASK_SIZE_ADDITION': otp.agg.tw_average('ASK_SIZE_ADDITION')},
                bucket_interval=otp.Minute(1))

result = otp.run(data,
                 start=otp.dt(2026, 7, 23, 9, 30),
                 end=otp.dt(2026, 7, 23, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## European Composite Bar Creation

Returns OHLC prices for the European Composite by Venue over the query interval.  
The query groups by `TRADE_VENUE`,
and since the European Composite includes trades in multiple currencies, also by `CURRENCY`.

Bars for Vodafone by Venue on `EU_COMP`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='EU_COMP_SAMPLE', tick_type='TRD')
data = data[['TRADE_VENUE', 'CURRENCY', 'PRICE', 'SIZE']]

# Notional value of each trade, summed per venue/currency below.
data['TRADE_VALUE'] = data['PRICE'] * data['SIZE']

data = data.agg({
    'FIRST_PRICE': otp.agg.first('PRICE'),
    'HIGH_PRICE': otp.agg.max('PRICE'),
    'LOW_PRICE': otp.agg.min('PRICE'),
    'LAST_PRICE': otp.agg.last('PRICE'),
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
    'VOLUME': otp.agg.sum('SIZE'),
    'TRADE_VALUE': otp.agg.sum('TRADE_VALUE'),
    'TRADE_COUNT': otp.agg.count(),
}, group_by=['TRADE_VENUE', 'CURRENCY'])

# EU_COMP Symbols are defined as ISINs
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 10, 23, 1),
                 end=otp.dt(2024, 1, 3, 10, 27, 48),
                 timezone='Europe/London',
                 symbols='GB00BH4HKS39')
result
```

### European Composite Bar Creation with Fill Forward

Returns OHLC prices for the European Composite by Venue with the last price filled forward into 5-minute
buckets.  
The query groups on `bucket_interval`, `TRADE_VENUE` and `CURRENCY`.  
The `LAST` aggregate is filled forward for buckets without trades by setting `time_series_type='state_ts'`.  
Trades are limited to those during Continuous Trading (`TRADE_PERIOD = '-'`)
and Lit Order Book Trades (`BOOK_TYPE = '0'`).

5 Minute Bars for Vodafone by Venue on `EU_COMP` with the `LAST_PRICE` filled Forward.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='EU_COMP', tick_type='TRD')
data = data[['TRADE_VENUE', 'CURRENCY', 'PRICE', 'SIZE', 'TRADE_PERIOD', 'BOOK_TYPE']]
# Regular Trades, Lit Order Book Trades
data = data.where((data['TRADE_PERIOD'] == '-') & (data['BOOK_TYPE'] == '0'))

data = data.agg({
    'FIRST_PRICE': otp.agg.first('PRICE'),
    'HIGH_PRICE': otp.agg.max('PRICE'),
    'LOW_PRICE': otp.agg.min('PRICE'),
    'LAST_PRICE': otp.agg.last('PRICE', time_series_type='state_ts'),
    'VWAP_PRICE': otp.agg.vwap('PRICE', 'SIZE'),
    'VOLUME': otp.agg.sum('SIZE'),
    'TRADE_COUNT': otp.agg.count(),
}, bucket_interval=300, group_by=['TRADE_VENUE', 'CURRENCY'])

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 3, 16),
                 timezone='Europe/London',
                 symbols='GB00BH4HKS39')
result
```

## European Composite Trade Date

European Composite Trade Records Consolidating Liquidity Across European Venues.

Symbols are defined as ISINs, since the Ticker Symbol is not consistent across European venues.  
Trades are recorded with their `TRADE_VENUE` and `CURRENCY`,
since an instrument may trade in multiple currencies,
along with standardized `TRADE_PERIOD` and `BOOK_TYPE` fields,
and MiFID-related fields such as `MMT_MKT_MECH` and `MMT_TRD_MODE`.  
Multiple symbols are requested at once by passing a list to the `symbols` parameter.

Retrieving Trades for All European Venues using `EU_COMP` Across 4 ISINs.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='EU_COMP_SAMPLE', tick_type='TRD')

data = otp.merge([data],
                 symbols=['GB00BH4HKS39', 'FR001400J770', 'DE0005140008', 'GB00BP6MXD84'],
                 identify_input_ts=True)
data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 3, 16),
                 timezone='Europe/London')
result
```

## European Composite Daily Bars

For the European Composite, returns pre-calculated Daily OHLCV (Open, High, Low, Close, Volume) data
for a specified symbol and date range.  
Multiple rows are returned per day: one per `VENUE_ID` and `CURRENCY`.  
The composite itself is represented as `VENUE_ID` with an empty string,
and records aggregated across currency are represented as `CURRENCY` with an empty string
(returning nan values for Price related fields).

European Composite Trade Daily Bars for Vodafone Including Volume Split by Trade Type,
and Divided by Venue and Currency.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='EU_COMP', tick_type='DAY')
data = data.limit(1000)

# EU_COMP Symbols are defined as ISINs
result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='GB00BH4HKS39')
result
```

## Quote to NBBO

Returns aggregated Durations of Exchange Quotes relative to the NBBO.  
Metrics include Durations in Seconds and Time Weighted Average Bid and Ask Sizes.  
Multiple rows are returned per day: one per `EXCHANGE`/`VENUE_ID`.

US Composite Quotes Relative to NBBO Daily Metrics.

```python
import onetick.py as otp

data = otp.DataSource(db='US_COMP_MKT_SHARE', tick_type='QTE_NBBO_DAY')
data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

{.dataframe}
|    |                Time | NBBO_DURATION | AT_NBBO_BOTH_DURATION | AT_NBBO_BID_DURATION | AT_NBBO_ASK_DURATION | ... | AT_NBBO_BOTH_SIZE | AT_NBBO_BID_SIZE | AT_NBBO_ASK_SIZE | AT_NBBO_EITHER_SIZE | EXCHANGE |
|---:|--------------------:|--------------:|----------------------:|---------------------:|---------------------:|----:|------------------:|-----------------:|-----------------:|--------------------:|---------:|
|  0 | 2024-01-03 20:15:00 |  23399.999042 |           3461.328841 |         12254.868121 |         11851.047136 | ... |          2.195534 |         1.071412 |         1.096154 |            1.265252 |        A |
|  1 | 2024-01-03 20:15:00 |  23399.999042 |              0.000000 |            91.206346 |          1049.890999 | ... |               NaN |         1.441661 |         1.301335 |            1.312551 |        B |
|  2 | 2024-01-03 20:15:00 |  23399.999042 |            166.372875 |          4855.599746 |          3486.597275 | ... |          3.899924 |         1.352153 |         1.692614 |            1.524859 |        C |
|  3 | 2024-01-03 20:15:00 |  23399.999042 |              0.000000 |             0.000000 |             0.000000 | ... |               NaN |              NaN |              NaN |                 NaN |        D |
|  4 | 2024-01-03 20:15:00 |  23399.999042 |              0.000000 |             0.000000 |             0.000000 | ... |               NaN |              NaN |              NaN |                 NaN |        F |
| .. |                 ... |           ... |                   ... |                  ... |                  ... | ... |               ... |              ... |              ... |                 ... |      ... |
| 18 | 2024-01-03 20:15:00 |  23399.999042 |          14193.489903 |         19143.794961 |         17967.336442 | ... |          9.484606 |         5.674532 |         5.009383 |            8.667443 |        V |
| 19 | 2024-01-03 20:15:00 |  23399.999042 |              0.000000 |             0.000000 |             0.000000 | ... |               NaN |              NaN |              NaN |                 NaN |        W |
| 20 | 2024-01-03 20:15:00 |  23399.999042 |            441.048628 |          3814.788175 |          4883.906510 | ... |          4.946489 |         3.624611 |         3.199336 |            3.566680 |        X |
| 21 | 2024-01-03 20:15:00 |  23399.999042 |            528.385826 |          9799.012940 |          7777.112852 | ... |          4.828393 |         3.575193 |         3.450817 |            3.629264 |        Y |
| 22 | 2024-01-03 20:15:00 |  23399.999042 |          21819.373044 |         22418.592538 |         22727.999920 | ... |         12.584441 |         6.201227 |         6.529836 |           12.321782 |        Z |

## Trade to NBBO

Returns aggregated Daily Volume, Value and Trade Counts for a specified symbol and date range for a Composite.  
Metrics include groupings by Trade Type, Block Trades, and Trades relative to the NBBO.  
Multiple rows are returned per day: one per `EXCHANGE`/`VENUE_ID`.

US Composite Trade Daily Metrics including Trades Relative to the NBBO and Block trades.

```python
import onetick.py as otp

data = otp.DataSource(db='US_COMP_MKT_SHARE', tick_type='TRD_NBBO_DAY')
data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

{.dataframe}
|    |                Time |  TRADE_VALUE | TRADE_COUNT |     VOLUME | TRADE_VALUE_MAIN_SESSION | ... | TRADE_VALUE_POST_MARKET | TRADE_VALUE_BLOCK | BLOCK_VOLUME | TRADE_COUNT_BLOCK |  EXCHANGE |
|---:|--------------------:|-------------:|------------:|-----------:|-------------------------:|----:|------------------------:|------------------:|-------------:|------------------:|----------:|
|  0 | 2024-01-03 20:15:00 | 1.024018e+09 |    130038.0 | 20303875.0 |             7.301464e+08 | ... |                   389.0 |      3.755104e+08 |    7438923.0 |             430.0 |           |
|  1 | 2024-01-03 20:15:00 | 1.002469e+06 |       236.0 |    19865.0 |             1.002469e+06 | ... |                     0.0 |      0.000000e+00 |          0.0 |               0.0 |         A |
|  2 | 2024-01-03 20:15:00 | 1.688385e+06 |       485.0 |    33528.0 |             1.688385e+06 | ... |                     0.0 |      0.000000e+00 |          0.0 |               0.0 |         B |
|  3 | 2024-01-03 20:15:00 | 5.759688e+06 |      1832.0 |   114102.0 |             5.759688e+06 | ... |                     0.0 |      0.000000e+00 |          0.0 |               0.0 |         C |
|  4 | 2024-01-03 20:15:00 | 2.967372e+08 |     37598.0 |  5885087.0 |             2.210986e+08 | ... |                   165.0 |      1.200495e+08 |    2378854.0 |             268.0 |         D |
| .. |                 ... |          ... |         ... |        ... |                      ... | ... |                     ... |               ... |          ... |               ... |       ... |
| 12 | 2024-01-03 20:15:00 | 2.665790e+07 |      5444.0 |   529155.0 |             2.665744e+07 | ... |                     1.0 |      2.228014e+06 |      44426.0 |               6.0 |         U |
| 13 | 2024-01-03 20:15:00 | 2.023853e+07 |      4408.0 |   401516.0 |             2.021446e+07 | ... |                     0.0 |      0.000000e+00 |          0.0 |               0.0 |         V |
| 14 | 2024-01-03 20:15:00 | 2.314178e+06 |       604.0 |    45859.0 |             2.314178e+06 | ... |                     0.0 |      0.000000e+00 |          0.0 |               0.0 |         X |
| 15 | 2024-01-03 20:15:00 | 7.833038e+06 |      1519.0 |   155311.0 |             7.813260e+06 | ... |                     3.0 |      6.067000e+05 |      12000.0 |               6.0 |         Y |
| 16 | 2024-01-03 20:15:00 | 4.958872e+07 |     11603.0 |   983836.0 |             4.947041e+07 | ... |                     9.0 |      5.727366e+05 |      11342.0 |               4.0 |         Z |

## Calculate a combined Point in Time Trade and NBBO Snapshot Across a Composite

A specific point in time is selected as the query interval.  
All symbols are retrieved by merging across {class}`otp.Symbols <onetick.py.Symbols>`.  
A lookback of up to 1 day (86400s) via `back_to_first_tick` returns the prevailing trade and NBBO
before the selected time, per symbol.  
As a Composite is being accessed, the `NBBO` tick type is queried rather than `QTE`.

Calculate Point in Time Trade & NBBO Snapshot for `US_COMP`.

```{code-cell} ipython3

import onetick.py as otp

snapshot_time = otp.dt(2024, 1, 3, 12, 30)

trd = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD', back_to_first_tick=86400)
trd = trd[['PRICE', 'SIZE']]
trd = trd.last()

nbbo = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO', back_to_first_tick=86400)
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE']]
nbbo = nbbo.last()

# Combine the prevailing trade and prevailing NBBO for each symbol into one tick. on='all' cross-joins the
# (at most one) trade tick with the (at most one) NBBO tick; unlike on='same_size', it degrades gracefully to
# no output for a symbol that lacks a prevailing trade or NBBO within the lookback window, rather than erroring.
data = otp.join(trd, nbbo, on='all')

# Merge the combined snapshot for every symbol into a single stream.
# identify_input_ts adds the SYMBOL_NAME of each tick to the output.
merged = otp.merge([data], symbols=otp.Symbols('US_COMP_SAMPLE', for_tick_type='TRD'), identify_input_ts=True)

result = otp.run(merged,
                 start=snapshot_time,
                 end=snapshot_time,
                 timezone='America/New_York')
result
```

## Calculate Spread and Mid from Composite Quotes Per Venue

Composite datasets include both quotes per venue/exchange and an NBBO.  
The `QTE` tick type includes the top of book quotes associated with each venue.  
`US_COMP` uses the `EXCHANGE` field; other composites use `QUOTE_VENUE`.

```text
SPREAD = ASK_PRICE - BID_PRICE
MID_PRICE = (BID_PRICE + ASK_PRICE) / 2
```

Calculate Spread and Mid Price by Exchange for `CSCO` from the `QTE` table.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='QTE')
data = data[['EXCHANGE', 'BID_PRICE', 'ASK_PRICE', 'BID_SIZE', 'ASK_SIZE']]
data = data.where((data['BID_SIZE'] > 0) & (data['ASK_SIZE'] > 0))

data['MID_PRICE'] = (data['BID_PRICE'] + data['ASK_PRICE']) / 2
data['SPREAD'] = data['ASK_PRICE'] - data['BID_PRICE']
data = data[['EXCHANGE', 'BID_PRICE', 'ASK_PRICE', 'MID_PRICE', 'SPREAD']]

data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

### Calculate Spread and Mid Statistics from a Composite Venues

`SPREAD` and `MID_PRICE` are computed per venue quote,
then aggregated across the period using the time weighted average, high and low, grouped by `EXCHANGE`.

Calculate Spread and Mid Price Aggregates by Exchange for `CSCO` from the `QTE` Table.

```{code-cell} ipython3

import onetick.py as otp

qte = otp.DataSource(db='US_COMP_SAMPLE', tick_type='QTE')
qte = qte[['EXCHANGE', 'BID_PRICE', 'ASK_PRICE', 'BID_SIZE', 'ASK_SIZE']]
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
}, group_by=['EXCHANGE'])

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Calculate Spread and Mid from a Composite NBBO

Composite datasets include both quotes per venue/exchange and an NBBO.  
The NBBO can either be regulatory (as in the US) or constructed.

```text
SPREAD = ASK_PRICE - BID_PRICE
MID_PRICE = (BID_PRICE + ASK_PRICE) / 2
```

Calculate Spread and Mid Price for `CSCO` from the `NBBO`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO')
data = data[['BID_PRICE', 'ASK_PRICE', 'BID_SIZE', 'ASK_SIZE']]
data = data.where((data['BID_SIZE'] > 0) & (data['ASK_SIZE'] > 0))

data['MID_PRICE'] = (data['BID_PRICE'] + data['ASK_PRICE']) / 2
data['SPREAD'] = data['ASK_PRICE'] - data['BID_PRICE']
data = data[['BID_PRICE', 'ASK_PRICE', 'MID_PRICE', 'SPREAD']]

data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

### Calculate Spread and Mid Statistics from a Composite NBBO

`SPREAD` and `MID_PRICE` are computed per `NBBO` quote,
then aggregated across the period using the time weighted average, high and low.

Calculate Spread and Mid Price Aggregates for `CSCO` from the `NBBO`.

```{code-cell} ipython3

import onetick.py as otp

nbbo = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO')
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE', 'BID_SIZE', 'ASK_SIZE']]
nbbo = nbbo.where((nbbo['BID_SIZE'] > 0) & (nbbo['ASK_SIZE'] > 0))

# Calculate the Mid Price and Spread
nbbo['MID_PRICE'] = (nbbo['BID_PRICE'] + nbbo['ASK_PRICE']) / 2
nbbo['SPREAD'] = nbbo['ASK_PRICE'] - nbbo['BID_PRICE']

data = nbbo.agg({
    'TW_AVG_MID_PRICE': otp.agg.tw_average('MID_PRICE'),
    'HIGH_MID_PRICE': otp.agg.max('MID_PRICE'),
    'LOW_MID_PRICE': otp.agg.min('MID_PRICE'),
    'TW_AVG_SPREAD': otp.agg.tw_average('SPREAD'),
    'HIGH_SPREAD': otp.agg.max('SPREAD'),
    'LOW_SPREAD': otp.agg.min('SPREAD'),
    'QTE_COUNT': otp.agg.count(),
})

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

### Calculate Rolling Spread and Mid Statistics from a Composite NBBO

`SPREAD` and `MID_PRICE` are computed per `NBBO` quote.  
Rolling statistics across the period calculate the time weighted average,
high and low at every point in time, using `running=True` with the default `bucket_interval=0`
(accumulating from the query start up to each quote).

Calculate Rolling Spread and Mid Price Aggregates for `CSCO` from the `NBBO`.

```{code-cell} ipython3

import onetick.py as otp

nbbo = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO')
nbbo = nbbo[['BID_PRICE', 'ASK_PRICE', 'BID_SIZE', 'ASK_SIZE']]
nbbo = nbbo.where((nbbo['BID_SIZE'] > 0) & (nbbo['ASK_SIZE'] > 0))

# Calculate the Mid Price and Spread
nbbo['MID_PRICE'] = (nbbo['BID_PRICE'] + nbbo['ASK_PRICE']) / 2
nbbo['SPREAD'] = nbbo['ASK_PRICE'] - nbbo['BID_PRICE']

data = nbbo.agg({
    'ROLLING_TW_AVG_MID_PRICE': otp.agg.tw_average('MID_PRICE'),
    'ROLLING_HIGH_MID_PRICE': otp.agg.max('MID_PRICE'),
    'ROLLING_LOW_MID_PRICE': otp.agg.min('MID_PRICE'),
    'ROLLING_TW_AVG_SPREAD': otp.agg.tw_average('SPREAD'),
    'ROLLING_HIGH_SPREAD': otp.agg.max('SPREAD'),
    'ROLLING_LOW_SPREAD': otp.agg.min('SPREAD'),
}, running=True, all_fields=True)

data = data[['BID_PRICE', 'ASK_PRICE', 'MID_PRICE', 'SPREAD',
             'ROLLING_TW_AVG_MID_PRICE', 'ROLLING_HIGH_MID_PRICE', 'ROLLING_LOW_MID_PRICE',
             'ROLLING_TW_AVG_SPREAD', 'ROLLING_HIGH_SPREAD', 'ROLLING_LOW_SPREAD']]

data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 16, 0),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```
