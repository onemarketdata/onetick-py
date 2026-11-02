---
file_format: mystnb
---

# Effective to Quoted Spread (ETQ)

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

`ETQ` is a widely-used metric for small order instantaneous price impact.  
It is essentially an easier-to-interpret Effective Spread that (when aggregated)
normalizes for the usual spread differences between instruments.  
ETQ is based on the regulatory Effective Spread Value, though ETQ is not itself a regulator-mandated metric.  
Like Effective Spread, ETQ is meaningful only for aggressive orders that execute immediately.  
Formula to get ETQ:

```text
ETQ = (VWAP - MID_PRICE) * 2 * DIRECTION / (ASK_PRICE - BID_PRICE)
```

The prevailing quote is joined to each order "as of".  
`DIRECTION` is +1 for BUY and -1 for SELL. `VWAP` is the fill VWAP across the order.

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_LSE_SAMPLE', tick_type='ORDER')
order = order[['ID', 'PRICE_FILLED', 'QTY_FILLED', 'SIDE']]
order['DIRECTION'] = order.if_else(order['SIDE'] == 'BUY', 1, -1)

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte['MID_PRICE'] = (qte['ASK_PRICE'] + qte['BID_PRICE']) / 2
qte = qte[['ASK_PRICE', 'BID_PRICE', 'MID_PRICE']]

joined = otp.join_by_time([order, qte])

data = joined.agg({
    'VWAP_PRICE': otp.agg.vwap('PRICE_FILLED', 'QTY_FILLED'),
    'ASK_PRICE': otp.agg.first('ASK_PRICE'),
    'BID_PRICE': otp.agg.first('BID_PRICE'),
    'MID_PRICE': otp.agg.first('MID_PRICE'),
    'DIRECTION': otp.agg.first('DIRECTION'),
}, group_by=['ID'])

data = data.where((data['ASK_PRICE'] - data['BID_PRICE']) != 0)
data['ETQ'] = (data['VWAP_PRICE'] - data['MID_PRICE']) * 2 * data['DIRECTION'] / (data['ASK_PRICE'] - data['BID_PRICE'])
data = data[['ID', 'ETQ']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |       Time |                 ID |       ETQ |
|---:|-----------:|-------------------:|----------:|
| 0  | 2024-01-04 | 233402424600089290 | -1.000000 |
| 1  | 2024-01-04 | 233402424600163510 | -0.500000 |
| 2  | 2024-01-04 | 233402424600242327 | -0.500000 |
| 3  | 2024-01-04 | 233402424600242348 | -1.000000 |
| 4  | 2024-01-04 | 233402424600242354 | -1.000000 |
| .. |        ... |                ... |       ... |
| 25 | 2024-01-04 | 233402424601044819 | -1.000000 |
| 26 | 2024-01-04 | 233402424601076779 |  0.333333 |
| 27 | 2024-01-04 | 233402424601108653 | -1.000000 |
| 28 | 2024-01-04 | 233402424601108753 |  1.000000 |
| 29 | 2024-01-04 | 233402424601134016 |  0.000000 |
