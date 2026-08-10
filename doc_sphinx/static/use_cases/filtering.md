---
file_format: mystnb
---

# Filtering

This section contains 8 examples for Filtering using the `onetick-py`.  
Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} webapi_configuration.py
```

## Adding Multiple Filters

Applying multiple filters to the dataset as two separate operations.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE', 'EXCHANGE']]
data['TRADED_VALUE'] = data['PRICE'] * data['SIZE']

data = data.where(data['EXCHANGE'] == 'N')
data = data.where(data['SIZE'] > 100)

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Filtering on a Single Trade Condition

Filtering that a trade condition is present in the ``COND`` field of the ``US_COMP_SAMPLE`` database.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data, _= data[data['COND'].str.contains('I')]
data = data[:100]
result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Filtering on Multiple Trade Conditions

Filtering that any of the specified trade conditions are present
in the ``COND`` field of the ``US_COMP`` database, by using {meth}`~onetick.py.Source.character_present`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data.character_present(data['COND'], 'O6TUHILNRWZ47QMBCGPV')
data = data[:100]
result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Filtering on Excluding Multiple Trade Conditions

Filtering that any of the specified trade conditions are not present
in the ``COND`` field of the ``US_COMP_SAMPLE`` database.  
Using {meth}`~onetick.py.Source.character_present` with parameter `discard_on_match=True`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data.character_present(data['COND'], 'O6TUHILNRWZ47QMBCGPV', discard_on_match=True)
data = data[:100]
result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Filtering on Specific Time Ranges

Applying a filter on the Data Source based on specific time periods per day,
using {meth}`~onetick.py.Source.time_filter`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data.time_filter(start_time='09:33:00', end_time='09:35:00')
data = data[:1000]
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Filtering on Excluding Specific Time Ranges

Applying a filter on the Data Source based on excluding specific time periods per day.  
Using {meth}`~onetick.py.Source.time_filter` with parameter `discard_on_match=True`.

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='US_COMP_SAMPLE', tick_type='TRD')
data = data.time_filter(start_time='09:30:00', end_time='09:33:00', discard_on_match=True)
data = data[:1000]
result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30),
                 end=otp.dt(2024, 1, 3, 9, 40),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

## Relative Time Filtering on the last 5 minutes Trades

Retrieving Trades for the last 5 minutes, using relative syntax for start and end in {func}`otp.run <onetick.py.run>`.

```python
import onetick.py as otp

data = otp.DataSource(db='US_COMP', tick_type='TRD')
data = data[['PRICE', 'SIZE', 'COND']]
data = data.limit(1000)
result = otp.run(data,
                 start=otp.now() - otp.Minute(5),
                 end=otp.now(),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

{.dataframe}
|     |                          Time |    PRICE | SIZE | COND |
|----:|------------------------------:|---------:|-----:|-----:|
|   0 | 2026-08-04 09:38:09.337531279 | 120.1068 |    1 | @  I |
|   1 | 2026-08-04 09:38:09.509870550 | 120.1200 |   14 | @  I |
|   2 | 2026-08-04 09:38:09.572110788 | 120.1500 |   10 | @  I |
|   3 | 2026-08-04 09:38:09.572151798 | 120.1500 |  398 |   @F |
|   4 | 2026-08-04 09:38:09.572172621 | 120.1500 |    1 | @  I |
| ... |                           ... |      ... |  ... |  ... |
| 995 | 2026-08-04 09:39:06.025000447 | 120.3100 |   20 | @F I |
| 996 | 2026-08-04 09:39:06.281626529 | 120.2871 |    1 | @  I |
| 997 | 2026-08-04 09:39:06.311070704 | 120.3138 |    1 | @  I |
| 998 | 2026-08-04 09:39:06.475337961 | 120.3050 |  100 |    @ |
| 999 | 2026-08-04 09:39:06.475912328 | 120.3050 |   17 | @  I |

1000 rows x 4 columns

## Relative Time Filtering on Trades from Today

Retrieving Trades from Today until Now, using relative syntax for start and end in {func}`otp.run <onetick.py.run>`.

```python
import onetick.py as otp

data = otp.DataSource(db='US_COMP', tick_type='TRD')
data = data[['PRICE', 'SIZE', 'COND']]
data = data.limit(1000)
result = otp.run(data,
                 # get the current date == start of day
                 start=otp.now().dt.date(),
                 end=otp.now(),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

{.dataframe}
|     |                          Time |    PRICE | SIZE | COND |
|----:|------------------------------:|---------:|-----:|-----:|
|   0 | 2026-08-04 04:00:00.062628594 | 115.8700 |   10 | @ TI |
|   1 | 2026-08-04 04:00:00.517801239 | 115.8700 |   28 | @ TI |
|   2 | 2026-08-04 04:00:01.572798398 | 116.2988 |    1 | @ TI |
|   3 | 2026-08-04 04:00:01.961360151 | 115.4412 |    1 | @ TI |
|   4 | 2026-08-04 04:00:02.804289238 | 115.8300 |    5 | @ TI |
| ... |                           ... |      ... |  ... |  ... |
| 995 | 2026-08-04 07:34:13.936435065 | 117.7400 |   18 | @FTI |
| 996 | 2026-08-04 07:34:13.936615045 | 117.7400 |    2 | @FTI |
| 997 | 2026-08-04 07:34:13.941759948 | 117.8000 |   37 | @FTI |
| 998 | 2026-08-04 07:34:13.941765195 | 117.8000 |   31 | @FTI |
| 999 | 2026-08-04 07:34:13.952417322 | 117.7800 |   34 | @ TI |

1000 rows x 4 columns
