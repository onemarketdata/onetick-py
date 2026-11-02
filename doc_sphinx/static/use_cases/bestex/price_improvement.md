---
file_format: mystnb
---

# Price Improvement

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

Price Improvement (bps) measures the distance from the far touch (FT) to the order execution VWAP in basis points.  
Positive values indicate a good execution, negative a poor one.

```text
PRICE_IMPROVEMENT_BPS = DIRECTION * 10000 * (FT - VWAP) / FT
```

`FT` is the arrival far touch by side (*BUY* -> arrival ask, *SELL* -> arrival bid).  
The prevailing quote at arrival is joined "as of" to the order.

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_LSE_SAMPLE', tick_type='ORDER')
order = order[['ID', 'SIDE', 'PRICE_FILLED', 'QTY_FILLED']]
order['DIRECTION'] = order.if_else(order['SIDE'] == 'BUY', 1, -1)

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte = qte[['ASK_PRICE', 'BID_PRICE']]

joined = otp.join_by_time([order, qte])

data = joined.agg({
    'VWAP_PRICE': otp.agg.vwap('PRICE_FILLED', 'QTY_FILLED'),
    'ARRIVAL_ASK_PRICE': otp.agg.first('ASK_PRICE'),
    'ARRIVAL_BID_PRICE': otp.agg.first('BID_PRICE'),
    'SIDE': otp.agg.first('SIDE'),
    'DIRECTION': otp.agg.first('DIRECTION'),
}, group_by=['ID'])

data['FT'] = data.if_else(data['SIDE'] == 'BUY', data['ARRIVAL_ASK_PRICE'], data['ARRIVAL_BID_PRICE'])

data = data.where(data['FT'] != 0)
data['PRICE_IMPROVEMENT_BPS'] = data['DIRECTION'] * 10000 * (data['FT'] - data['VWAP_PRICE']) / data['FT']
data = data[['ID', 'PRICE_IMPROVEMENT_BPS']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |       Time |                 ID | PRICE_IMPROVEMENT_BPS |
|---:|-----------:|-------------------:|----------------------:|
|  0 | 2024-01-04 | 233402424600089290 |              5.681011 |
|  1 | 2024-01-04 | 233402424600163510 |              4.266818 |
|  2 | 2024-01-04 | 233402424600242327 |              4.241482 |
|  3 | 2024-01-04 | 233402424600242348 |              5.658509 |
|  4 | 2024-01-04 | 233402424600242354 |              5.658509 |
| .. |        ... |                ... |                   ... |
| 25 | 2024-01-04 | 233402424601044819 |              1.428980 |
| 26 | 2024-01-04 | 233402424601076779 |              1.431025 |
| 27 | 2024-01-04 | 233402424601108653 |              1.429388 |
| 28 | 2024-01-04 | 233402424601108753 |             -0.000000 |
| 29 | 2024-01-04 | 233402424601134016 |              1.431229 |
