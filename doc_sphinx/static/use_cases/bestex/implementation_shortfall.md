---
file_format: mystnb
---

# Implementation Shortfall (Slippage)

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

Implementation Shortfall (`IS`), or slippage --
measures execution quality relative to the mid prevailing when the order arrived.  
Large positive values imply a good execution; large negative values a poor one.

To calculate the Implementation Shortfall (IS) for each order,
we need to determine the direction of the order, the executed quantity (sum of QTY_FILLED),
the arrival mid-price, and the Volume Weighted Average Price (VWAP) of the order.

```text
IS = DIRECTION * EXEC_QTY * (ARRIVAL_MID_PRICE - VWAP)
```

The prevailing quote is joined to each order "as of"; the arrival mid is the first mid seen for the order.

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_LSE_SAMPLE', tick_type='ORDER')
order = order[['ID', 'PRICE_FILLED', 'QTY_FILLED', 'SIDE']]
order['DIRECTION'] = order.if_else(order['SIDE'] == 'BUY', 1, -1)

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte['MID_PRICE'] = (qte['ASK_PRICE'] + qte['BID_PRICE']) / 2
qte = qte[['MID_PRICE']]

joined = otp.join_by_time([order, qte])

data = joined.agg({
    'ARRIVAL_MID_PRICE': otp.agg.first('MID_PRICE'),
    'VWAP_PRICE': otp.agg.vwap('PRICE_FILLED', 'QTY_FILLED'),
    'EXEC_QTY': otp.agg.sum('QTY_FILLED'),
    'DIRECTION': otp.agg.first('DIRECTION'),
}, group_by=['ID'])

data = data.where(data['EXEC_QTY'] > 0)
data['IS'] = data['DIRECTION'] * data['EXEC_QTY'] * (data['ARRIVAL_MID_PRICE'] - data['VWAP_PRICE'])
data = data[['ID', 'IS']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |       Time |                 ID |     IS |
|---:|-----------:|-------------------:|-------:|
|  0 | 2024-01-04 | 233402424600089290 | 39.740 |
|  1 | 2024-01-04 | 233402424600163510 | 12.140 |
|  2 | 2024-01-04 | 233402424600242327 |  7.500 |
|  3 | 2024-01-04 | 233402424600242348 | 15.000 |
|  4 | 2024-01-04 | 233402424600242354 | 15.000 |
| .. |        ... |                ... |    ... |
| 25 | 2024-01-04 | 233402424601044819 |  7.115 |
| 26 | 2024-01-04 | 233402424601076779 | -0.375 |
| 27 | 2024-01-04 | 233402424601108653 |  4.995 |
| 28 | 2024-01-04 | 233402424601108753 | -0.025 |
| 29 | 2024-01-04 | 233402424601134016 |  0.000 |
