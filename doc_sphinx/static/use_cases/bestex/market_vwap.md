---
file_format: mystnb
---

# Interval Market VWAP

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

The market VWAP is the volume weighted average trade price over an interval, a common benchmark for order execution.  
This example computes a single market VWAP over a fixed interval; to benchmark a specific order,
restrict the interval to that order's arrival / exit times.

```text
MARKET_VWAP = VWAP(PRICE, SIZE)
```

```{code-cell} ipython3

import onetick.py as otp

data = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD')
data = data[['PRICE', 'SIZE']]

data = data.agg({
    'MARKET_VWAP': otp.agg.vwap('PRICE', 'SIZE'),
    'VOLUME': otp.agg.sum('SIZE'),
    'TRADE_COUNT': otp.agg.count(),
})

result = otp.run(data,
                 start=otp.dt(2024, 1, 3, 9, 30, 0),
                 end=otp.dt(2024, 1, 3, 9, 31, 0),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

## Computing market VWAP for every order’s arrival/exit interval

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_LSE_SAMPLE', tick_type='ORDER')
order = order.agg({
    'ARRIVAL_TIME': otp.agg.first_time(),
    'EXIT_TIME': otp.agg.last_time(),
}, group_by=['ID'])

trd = otp.DataSource(db='LSE_SAMPLE', tick_type='TRD')
trd = trd.agg({'MARKET_VWAP': otp.agg.vwap('PRICE', 'SIZE')})

data = order.join_with_query(trd, start=order['ARRIVAL_TIME'], end=order['EXIT_TIME'])
data = data[['ID', 'ARRIVAL_TIME', 'EXIT_TIME', 'MARKET_VWAP']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |       Time |                 ID |            ARRIVAL_TIME |               EXIT_TIME | MARKET_VWAP |
|---:|-----------:|-------------------:|------------------------:|------------------------:|------------:|
|  0 | 2024-01-04 | 233402424600089290 | 2024-01-03 08:08:07.502 | 2024-01-03 08:11:04.376 |   70.465750 |
|  1 | 2024-01-04 | 233402424600163510 | 2024-01-03 08:27:05.220 | 2024-01-03 08:27:09.271 |   70.280000 |
|  2 | 2024-01-04 | 233402424600242327 | 2024-01-03 08:53:19.672 | 2024-01-03 08:53:19.675 |   70.711599 |
|  3 | 2024-01-04 | 233402424600242348 | 2024-01-03 08:53:19.681 | 2024-01-03 08:53:19.683 |   70.730000 |
|  4 | 2024-01-04 | 233402424600242354 | 2024-01-03 08:53:19.685 | 2024-01-03 08:53:19.687 |   70.730000 |
| .. |        ... |                ... |                     ... |                     ... |         ... |
| 25 | 2024-01-04 | 233402424601044819 | 2024-01-03 15:12:01.813 | 2024-01-03 15:12:01.813 |         NaN |
| 26 | 2024-01-04 | 233402424601076779 | 2024-01-03 15:21:17.758 | 2024-01-03 15:21:24.450 |   69.890000 |
| 27 | 2024-01-04 | 233402424601108653 | 2024-01-03 15:31:59.347 | 2024-01-03 15:32:01.227 |   69.959119 |
| 28 | 2024-01-04 | 233402424601108753 | 2024-01-03 15:32:01.227 | 2024-01-03 15:32:04.972 |   69.944856 |
| 29 | 2024-01-04 | 233402424601134016 | 2024-01-03 15:38:33.230 | 2024-01-03 15:38:33.493 |   69.865039 |
