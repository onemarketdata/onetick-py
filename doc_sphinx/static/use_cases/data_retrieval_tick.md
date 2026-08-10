---
file_format: mystnb
---

# Data Retrieval - Tick

This section contains 6 examples for Data Retrieval - Tick using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Trades

Retrieve Exchange trades from the ``US_COMP_SAMPLE`` database for ``CSCO``, across the specified time range.

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

## Quotes

Retrieve Exchange quotes from the ``US_COMP_SAMPLE`` database for ``CSCO``, across the specified time range.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='QTE')
# Return first 100 Rows
data = data.limit(100)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## NBBO

Retrieve NBBO quotes from the ``US_COMP_SAMPLE`` database for ``CSCO``, across the specified time range.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO')
# Return first 100 Rows
data = data.limit(100)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Indicative Prices

Retrieve Indicative Auction Prices from the ``LSE_SAMPLE`` database for ``VOD``, across the specified time range.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='IND')
# Return first 100 Rows
data = data.limit(100)
result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Market Phases

Retrieve Exchange market phase changes from the ``LSE_SAMPLE`` database for ``VOD``, across the specified time range.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='MKT')

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Short Interest

Query short interest data for ``AAPL`` over the last 7 days.  
Returns all available short interest metrics for the specified date range.

```python
import onetick.py as otp

data = otp.DataSource(db='US_SHORT_INT', tick_type='DAY')

result = otp.run(data,
                 symbols='AAPL',
                 start=otp.now() - otp.Day(7),
                 end=otp.now(),
                 timezone='America/New_York')

# Display the result
result
```

{.dataframe}
|   |                Time | SHORT_VOLUME | SHORT_EXEMPT_VOLUME |      VOLUME | TRF | OMDSEQ |
|--:|--------------------:|-------------:|--------------------:|------------:|----:|-------:|
| 0 | 2026-07-27 20:15:00 |  7.12266e+06 |               36753 | 1.76945e+07 | BQN |     30 |
| 1 | 2026-07-28 20:15:00 |  9.60599e+06 |               42284 | 1.90899e+07 | BQN |     32 |
| 2 | 2026-07-29 20:15:00 |  8.47753e+06 |               43470 | 1.76618e+07 | BQN |     30 |
| 3 | 2026-07-30 20:15:00 |  9.69986e+06 |               39078 | 2.02463e+07 | BQN |     29 |
| 4 | 2026-07-31 20:15:00 |  2.36486e+07 |              822016 | 4.49851e+07 | BQN |     31 |
