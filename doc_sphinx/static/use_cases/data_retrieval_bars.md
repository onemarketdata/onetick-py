---
file_format: mystnb
---

# Data Retrieval - Bars

This section contains examples for Data Retrieval - Bars using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Daily Trade Bars

Retrieve Daily OHLC Records from the ``US_COMP_SAMPLE_DAILY`` database, and ``DAY`` table.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_DAILY', tick_type='DAY')
result = otp.run(data,
                 start=otp.dt(2024, 1, 1),
                 end=otp.dt(2024, 2, 1),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Minute Trade Bars

Retrieve pre-calculated 1 minute Trade Bars from the ``US_COMP_SAMPLE_BARS`` database, and ``TRD_1M`` table.  
Bars are calculated at the end of each minute, so setting time range from 09:31 to 16:01.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='TRD_1M')
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 31),
                 end=otp.dt(2024, 1, 3, 16, 1),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Minute Quote Bars

Retrieve pre-calculated 1 minute Quote Bars from the ``US_COMP_SAMPLE_BARS`` database, and ``QTE_1M`` table.
Bars are calculated at the end of each minute, so setting time range from 09:31 to 16:01.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE_BARS', tick_type='QTE_1M')
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 31),
                 end=otp.dt(2024, 1, 3, 16, 1),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Trade Daily Bars

Returns aggregated Daily OHLCV (Open, High, Low, Close, Volume) data for a specified symbol and date range.

LSE Trade Daily Bars Including Volume Split by Trade Type.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE_DAILY', tick_type='DAY')
data = data.limit(1000)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 2, 4),
                 timezone='UTC',
                 symbols='VOD')
result
```
