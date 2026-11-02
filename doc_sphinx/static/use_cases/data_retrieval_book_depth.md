---
file_format: mystnb
---

# Data Retrieval - Book Depth

This section contains examples for Data Retrieval - Book Depth using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Book Depth at Quantity

Calculate Book Depth Metrics such as Bid and Ask VWAP to trade 1000 shares every 60 seconds.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_summary(bucket_interval=60, max_depth_shares=1000)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 3, 16),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Book Depth at Time

Retrieving the Book to MBL (Market by Level) at a specified time.  
``PRL_FULL`` indicates that the table is actually a MBO (Market by Order) data set.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_snapshot()
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 12),
                 end=otp.dt(2024, 1, 3, 12),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Book Depth at Time to Max Levels

Retrieving the Book to MBO (Market by Order) at a specified time.  
The returned book is limited to a maximum number of price levels.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_snapshot(max_levels=5)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 12),
                 end=otp.dt(2024, 1, 3, 12),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Book Depth at Time to Max Price Skew

Retrieving the Book to MBO (Market by Order) at a specified time.  
The returned book is limited to a maximum price skew ``0.005 = 0.5%``.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_snapshot(max_depth_for_price=0.005)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 12),
                 end=otp.dt(2024, 1, 3, 12),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Book Depth at Time to Max Shares

Retrieving the Book to MBO (Market by Order) at a specified time.  
The returned book is limited to a maximum amount of accumulated size.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_snapshot(max_depth_shares=10000)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 12),
                 end=otp.dt(2024, 1, 3, 12),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Book Depth at Time to Max Spread

Retrieving the Book to MBO (Market by Order) at a specified time.  
The returned book is limited to a maximum absolute spread.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_snapshot(max_spread=1)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 12),
                 end=otp.dt(2024, 1, 3, 12),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Book Depth to MBO at Time

Retrieving the Book to MBO (Market by Order) at a specified time.  
Using parameter ``show_full_detail=True``.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_snapshot(show_full_detail=True)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 12),
                 end=otp.dt(2024, 1, 3, 12),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Consolidated Book at Time by Database List

Returning a Consolidated Book at a Point in Time, specifying 1 symbol and 3 different databases.  
The same instrument trades on several venues, each held in its own database.  
Passing the list of databases to {class}`otp.ObSnapshot <onetick.py.ObSnapshot>`
builds a single order book across all of them for the one symbol.  
The schema is forced for the venue specific fields such as `ORDER_ID` and `ORIGINAL_ORDER_ID`.

```python
import onetick.py as otp

ob_snap = otp.ObSnapshot(
    db=['NYSE', 'NASDAQ', 'ARCA'],
    tick_type='PRL_FULL',
    symbol='CSCO',
    show_full_detail=True,
    max_levels=5,
    # Force schema for venue specific fields such as ORDER_ID and ORIGINAL_ORDER_ID
    schema={'ORDER_ID': str, 'ORIGINAL_ORDER_ID': str},
)
ob_snap = ob_snap[['PRICE', 'SIZE', 'LEVEL', 'UPDATE_TIME', 'BUY_SELL_FLAG', 'SOURCE',
                   'ORDER_ID', 'ORIGINAL_ORDER_ID']]

result = otp.run(ob_snap,
                 start=otp.dt(2026, 9, 1, 12),
                 end=otp.dt(2026, 9, 1, 12),
                 timezone='America/New_York')
result
```

{.dataframe}
|    |                Time |  PRICE | SIZE | LEVEL |                   UPDATE_TIME | BUY_SELL_FLAG |       SOURCE |         ORDER_ID | ORIGINAL_ORDER_ID |
|---:|--------------------:|-------:|-----:|------:|------------------------------:|--------------:|-------------:|-----------------:|------------------:|
|  0 | 2026-09-01 12:00:00 | 109.14 |   20 |     1 | 2026-09-01 11:59:52.720710173 |             1 |   ARCA::CSCO | 3099523321641018 |  3099523321641018 |
|  1 | 2026-09-01 12:00:00 | 109.14 |    4 |     1 | 2026-09-01 11:59:59.472127565 |             1 |   ARCA::CSCO | 3099523321657713 |  3099523321657713 |
|  2 | 2026-09-01 12:00:00 | 109.14 |  600 |     1 | 2026-09-01 11:59:49.151792734 |             1 | NASDAQ::CSCO |        677932018 |         677932018 |
|  3 | 2026-09-01 12:00:00 | 109.14 |  100 |     1 | 2026-09-01 11:59:51.094427442 |             1 | NASDAQ::CSCO |        678038646 |         678038646 |
|  4 | 2026-09-01 12:00:00 | 109.15 |  100 |     2 | 2026-09-01 11:59:41.717908510 |             1 |   ARCA::CSCO | 3099523321609734 |  3099523321609734 |
| .. |                 ... |    ... |  ... |   ... |                           ... |           ... |          ... |              ... |               ... |
| 92 | 2026-09-01 12:00:00 | 109.09 |   11 |     5 | 2026-09-01 11:59:19.063619465 |             0 |   NYSE::CSCO | 3941749208074032 |  3941749208074032 |
| 93 | 2026-09-01 12:00:00 | 109.09 |   10 |     5 | 2026-09-01 11:59:19.063606052 |             0 |   ARCA::CSCO | 3099523321534917 |  3099523321534917 |
| 94 | 2026-09-01 12:00:00 | 109.09 |  100 |     5 | 2026-09-01 11:58:53.451079951 |             0 |   ARCA::CSCO | 3099523321475913 |  3099523321475913 |
| 95 | 2026-09-01 12:00:00 | 109.09 |    4 |     5 | 2026-09-01 11:58:50.007440425 |             0 |   ARCA::CSCO | 3099523321467831 |  3099523321467831 |
| 96 | 2026-09-01 12:00:00 | 109.09 |   25 |     5 | 2026-09-01 11:58:45.632718122 |             0 |   ARCA::CSCO | 3099523321455429 |  3099523321455429 |

### Consolidated Book at Time by Symbol List

Returning a Consolidated Book at a Point in Time,
specifying 3 different symbols (the same company across 3 different databases).  
Each venue holds the instrument in its own database, so the symbol is qualified with the database name using
the `DB::SYMBOL` form.  
{class}`otp.ObSnapshot <onetick.py.ObSnapshot>` builds a single order book across all of them.  
As no single db is given, the schema cannot be inferred and is specified explicitly.  
`SOURCE` must be present in the tick descriptor
whenever `show_full_detail` is set and the order book has multiple inputs.

```python
import onetick.py as otp

ob_snap = otp.ObSnapshot(
    tick_type='PRL_FULL',
    symbol=['NYSE::CSCO', 'NASDAQ::CSCO', 'ARCA::CSCO'],
    show_full_detail=True,
    max_levels=5,
    schema={'PRICE': float,
            'SIZE': int,
            'BUY_SELL_FLAG': int,
            'DELETED_TIME': otp.nsectime,
            'ORDER_ID': str,
            'ORIGINAL_ORDER_ID': str,
            'SOURCE': otp.string[32]},
)

result = otp.run(ob_snap,
                 start=otp.dt(2026, 9, 1, 12),
                 end=otp.dt(2026, 9, 1, 12),
                 timezone='America/New_York')
result
```

{.dataframe}
|    |                Time |  PRICE | SIZE | BUY_SELL_FLAG |         ORDER_ID | ORIGINAL_ORDER_ID | ... | OLD_PRICE_LEVEL | ORDER_POSITION | OLD_ORDER_POSITION | SIZE_AHEAD | RECORD_TYPE |
|---:|--------------------:|-------:|-----:|--------------:|-----------------:|------------------:|----:|----------------:|---------------:|-------------------:|-----------:|------------:|
|  0 | 2026-09-01 12:00:00 | 109.14 |   20 |             1 | 3099523321641018 |  3099523321641018 | ... |               0 |              3 |                  0 |         29 |           R |
|  1 | 2026-09-01 12:00:00 | 109.14 |    4 |             1 | 3099523321657713 |  3099523321657713 | ... |               0 |              2 |                  0 |         20 |           R |
|  2 | 2026-09-01 12:00:00 | 109.14 |  600 |             1 |        677932018 |         677932018 | ... |               0 |              5 |                  0 |        108 |           R |
|  3 | 2026-09-01 12:00:00 | 109.14 |  100 |             1 |        678038646 |         678038646 | ... |               0 |              2 |                  0 |        600 |           R |
|  4 | 2026-09-01 12:00:00 | 109.15 |  100 |             1 | 3099523321609734 |  3099523321609734 | ... |               0 |              8 |                  0 |         82 |           R |
| .. |                 ... |    ... |  ... |           ... |              ... |               ... | ... |             ... |            ... |                ... |        ... |         ... |
| 92 | 2026-09-01 12:00:00 | 109.09 |   11 |             0 | 3941749208074032 |  3941749208074032 | ... |               0 |              1 |                  0 |          0 |           R |
| 93 | 2026-09-01 12:00:00 | 109.09 |   10 |             0 | 3099523321534917 |  3099523321534917 | ... |               0 |             17 |                  0 |        920 |           R |
| 94 | 2026-09-01 12:00:00 | 109.09 |  100 |             0 | 3099523321475913 |  3099523321475913 | ... |               0 |              8 |                  0 |        354 |           R |
| 95 | 2026-09-01 12:00:00 | 109.09 |    4 |             0 | 3099523321467831 |  3099523321467831 | ... |               0 |              2 |                  0 |         25 |           R |
| 96 | 2026-09-01 12:00:00 | 109.09 |   25 |             0 | 3099523321455429 |  3099523321455429 | ... |               0 |              2 |                  0 |          4 |           R |

## Order Book Bars

Calculate 1 minute bars for book depth down to 5 levels from the ``LSE_SAMPLE`` database, and ``PRL_FULL`` table.  
Each output row showing a Book Level, Side and Time (Bid and Ask on different rows).

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_snapshot(bucket_interval=60, max_levels=5)
# Return first 100 Rows
data = data.limit(100)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 3, 16),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Order Book Flat Bars

Calculate 1 minute bars for book depth down to 5 levels from the ``LSE_SAMPLE`` database, and ``PRL_FULL`` table.  
Each output row showing the book at a specific time (many output columns for bids and asks at selected levels).

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_snapshot_flat(bucket_interval=60, max_levels=5)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 3, 16),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Order Book Updates

Retrieve the changes in the Book across time.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_snapshot(running=True, show_only_changes=True)
# Return first 100 Rows
data = data.limit(100)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 3, 9),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Order Book Wide Bars

Calculate 1 minute bars for book depth down to 5 levels from the ``LSE_SAMPLE`` database, and ``PRL_FULL`` table.  
Each output row showing a Book Level and Time (Bid and Ask on same row).

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='PRL_FULL')
data = data.ob_snapshot_wide(bucket_interval=60, max_levels=5)
# Return first 100 Rows
data = data.limit(100)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 8),
                 end=otp.dt(2024, 1, 3, 16),
                 timezone='Europe/London',
                 symbols='VOD')
result
```
