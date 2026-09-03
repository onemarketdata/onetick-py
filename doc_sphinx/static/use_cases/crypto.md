---
file_format: mystnb
---

# Crypto

This section contains 7 examples for Crypto using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Crypto Trade Retrieval

Retrieving Trades from a Crypto Venue.  
Unlike Equities and Futures, the Trade Size on a crypto venue is fractional.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='BINANCE', tick_type='TRD')
data = data.limit(1000)
result = otp.run(data,
                 start=otp.dt(2026, 7, 28),
                 end=otp.dt(2026, 7, 29),
                 timezone='UTC',
                 symbols='BTCUSD')
result
```

## Crypto Quote Retrieval

Retrieving Quotes from a Crypto Venue.  
Unlike Equities and Futures, the Bid and Ask Sizes on a crypto venue are fractional.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='BINANCE', tick_type='QTE')
data = data.limit(1000)
result = otp.run(data,
                 start=otp.dt(2026, 7, 28),
                 end=otp.dt(2026, 7, 29),
                 timezone='UTC',
                 symbols='BTCUSD')
result
```

## Crypto Book Update Retrieval

Retrieving Book Updates from a Crypto Venue.  
Book Updates are provided as an L2 dataset, providing updates to Price Levels.  
Basic retrieval is useful for counting order book changes.  
To reconstruct the order book, order book aggregations like {meth}`~onetick.py.Source.ob_snapshot_wide` should be used.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='BINANCE', tick_type='PRL')
data = data.limit(1000)
result = otp.run(data,
                 start=otp.dt(2026, 7, 28),
                 end=otp.dt(2026, 7, 29),
                 timezone='UTC',
                 symbols='BTCUSD')
result
```

## Crypto Book Snapshot Retrieval

Retrieving a Book Snapshot at a Specified Time from a Crypto Venue.  
To reconstruct the order book, the {meth}`~onetick.py.Source.ob_snapshot_wide` aggregation is used.  
As this is a crypto book, the `size_max_fractional_digits` attribute is set, allowing the book
to be reconstructed with size stored with up to 9 fractional digits.  
{meth}`~onetick.py.Source.ob_snapshot_wide` returns the book with Bid and Ask on the same row.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='BINANCE', tick_type='PRL')
data = data.ob_snapshot_wide(size_max_fractional_digits=9)
data = data[['BID_PRICE', 'BID_SIZE', 'ASK_PRICE', 'ASK_SIZE', 'LEVEL']]
result = otp.run(data,
                 start=otp.dt(2026, 7, 28, 12),
                 end=otp.dt(2026, 7, 28, 12),
                 timezone='UTC',
                 symbols='BTCUSD')
result
```

## Crypto Book Snapshot Retrieval with Accumulative Values

Retrieving a Book Snapshot at a Specified Time from a Crypto Venue, outputting Accumulative Depth.  
To reconstruct the order book, the {meth}`~onetick.py.Source.ob_snapshot_wide` aggregation is used.  
As this is a crypto book, the `size_max_fractional_digits` attribute is set.  
{meth}`~onetick.py.Source.ob_snapshot_wide` returns the book in a format with Bid and Ask on the same row.  
Bid and Ask Value is calculated using `PRICE * SIZE`.  
The Bid and Ask Sizes are used to calculate accumulative sizes across the book depth, computed with a
running sum aggregation across the levels.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='BINANCE', tick_type='PRL')
data = data.ob_snapshot_wide(size_max_fractional_digits=9)

# Value per level = PRICE * SIZE
data['BID_VALUE'] = data['BID_PRICE'] * data['BID_SIZE']
data['ASK_VALUE'] = data['ASK_PRICE'] * data['ASK_SIZE']

# Accumulative sizes across the book depth
data = data.agg({'ACCUM_BID_SIZE': otp.agg.sum('BID_SIZE'),
                 'ACCUM_ASK_SIZE': otp.agg.sum('ASK_SIZE')},
                running=True, all_fields=True)

data = data[['BID_PRICE', 'BID_SIZE', 'ASK_PRICE', 'ASK_SIZE', 'LEVEL',
             'BID_VALUE', 'ASK_VALUE', 'ACCUM_BID_SIZE', 'ACCUM_ASK_SIZE']]
result = otp.run(data,
                 start=otp.dt(2026, 7, 28, 12),
                 end=otp.dt(2026, 7, 28, 12),
                 timezone='UTC',
                 symbols='BTCUSD')
result
```

## Crypto Book Snapshot Retrieval with Accumulative Values and Best Prices

Retrieving a Book Snapshot at a Specified Time from a Crypto Venue, outputting Accumulative Depth and Best Prices.  
To reconstruct the order book, the {meth}`~onetick.py.Source.ob_snapshot_wide` aggregation is used.  
As this is a crypto book, the `size_max_fractional_digits` attribute is set.  
{meth}`~onetick.py.Source.ob_snapshot_wide` returns the book in a format with Bid and Ask on the same row.  
Bid and Ask Value is calculated using `PRICE * SIZE`.  
The Bid and Ask Sizes are used to calculate accumulative sizes across the book depth, computed with a
running sum aggregation across the levels.  
The Bid and Ask Prices are used to return the Best Prices across the book depth, computed with a
running first aggregation across the levels.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='BINANCE', tick_type='PRL')
data = data.ob_snapshot_wide(size_max_fractional_digits=9)

# Value per level = PRICE * SIZE
data['BID_VALUE'] = data['BID_PRICE'] * data['BID_SIZE']
data['ASK_VALUE'] = data['ASK_PRICE'] * data['ASK_SIZE']

# Accumulative sizes and best (first) prices across the book depth
data = data.agg({'ACCUM_BID_SIZE': otp.agg.sum('BID_SIZE'),
                 'ACCUM_ASK_SIZE': otp.agg.sum('ASK_SIZE'),
                 'BEST_BID_PRICE': otp.agg.first('BID_PRICE'),
                 'BEST_ASK_PRICE': otp.agg.first('ASK_PRICE')},
                running=True, all_fields=True)

data = data[['BID_PRICE', 'BID_SIZE', 'ASK_PRICE', 'ASK_SIZE', 'LEVEL',
             'BID_VALUE', 'ASK_VALUE', 'ACCUM_BID_SIZE', 'ACCUM_ASK_SIZE',
             'BEST_BID_PRICE', 'BEST_ASK_PRICE']]
result = otp.run(data,
                 start=otp.dt(2026, 7, 28, 12),
                 end=otp.dt(2026, 7, 28, 12),
                 timezone='UTC',
                 symbols='BTCUSD')
result
```

## Crypto Book Depth Statistics to Trade a Specified Amount Across Time

Calculating Bid and Ask VWAP and other Statistics across time from a Crypto Venue.  
To calculate Bid and Ask VWAP, the {meth}`~onetick.py.Source.ob_summary` aggregation is used.  
As this is a crypto book, the `size_max_fractional_digits` attribute is set.  
The `max_depth_shares` attribute determines how much should be traded.  
The `bucket_interval` attribute determines how often to output the resulting book metrics.  

The returned `BID_VWAP` and `ASK_VWAP` can be used to calculate Effective Spread.  
The returned `BID_SIZE` and `ASK_SIZE` identify if the liquidity is present.  
The returned `BEST_ASK_PRICE` and `BEST_BID_PRICE` can be used to calculate the Price Skew together
with the `BID_VWAP` and `ASK_VWAP`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='BINANCE', tick_type='PRL')
data = data.ob_summary(size_max_fractional_digits=9,
                       bucket_interval=60,
                       max_depth_shares=0.5)
result = otp.run(data,
                 start=otp.dt(2026, 7, 28),
                 end=otp.dt(2026, 7, 29),
                 timezone='UTC',
                 symbols='BTCUSD')
result
```
