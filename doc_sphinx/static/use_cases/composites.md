---
file_format: mystnb
---

# Composites

This section contains 11 examples for Composites using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
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

## US Odd Lot NBBO Retrieval

Retrieving the US NBBO including Odd Lots.  
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

## US Odd Lot Quote Retrieval

Retrieving the US Exchange Quotes including Odd Lots.  
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

## Joining NBBOs with and without Odd Lots

Retrieving the US Exchange NBBO with and without Odd Lots (BOLO).  
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

### Joining NBBOs with and without Odd Lots and Calculating Skews and Liquidity

Retrieving the US Exchange NBBO with and without Odd Lots, calculating Skews and Liquidity.  
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

## TWAP Bars for Spreads, Skews and Added Liquidity Comparing NBBOs with and without Odd Lots

Calculating 1 Minute TWAP Bars for Spreads, Skews and added liquidity comparing NBBOs with and without Odd Lots.  
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
