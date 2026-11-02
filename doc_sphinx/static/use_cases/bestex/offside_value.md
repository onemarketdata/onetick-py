---
file_format: mystnb
---

# Offside Value

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

Offside value measures the impact of an aggressive order: the distance in notional units that the impacted
side moved from the far touch prevailing when the order arrived.

```text
OFFSIDE_VALUE = EXEC_QTY * (VWAP - FT) * DIRECTION
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
    'EXEC_QTY': otp.agg.sum('QTY_FILLED'),
    'ARRIVAL_ASK_PRICE': otp.agg.first('ASK_PRICE'),
    'ARRIVAL_BID_PRICE': otp.agg.first('BID_PRICE'),
    'SIDE': otp.agg.first('SIDE'),
    'DIRECTION': otp.agg.first('DIRECTION'),
}, group_by=['ID'])

data['FT'] = data.if_else(data['SIDE'] == 'BUY', data['ARRIVAL_ASK_PRICE'], data['ARRIVAL_BID_PRICE'])

data = data.where(data['EXEC_QTY'] > 0)
data['OFFSIDE_VALUE'] = data['EXEC_QTY'] * (data['VWAP_PRICE'] - data['FT']) * data['DIRECTION']
data = data[['ID', 'OFFSIDE_VALUE']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |       Time |                 ID | OFFSIDE_VALUE |
|---:|-----------:|-------------------:|--------------:|
|  0 | 2024-01-04 | 233402424600089290 |        -79.48 |
|  1 | 2024-01-04 | 233402424600163510 |        -36.42 |
|  2 | 2024-01-04 | 233402424600242327 |        -22.50 |
|  3 | 2024-01-04 | 233402424600242348 |        -30.00 |
|  4 | 2024-01-04 | 233402424600242354 |        -30.00 |
| .. |        ... |                ... |           ... |
| 25 | 2024-01-04 | 233402424601044819 |        -14.23 |
| 26 | 2024-01-04 | 233402424601076779 |         -0.75 |
| 27 | 2024-01-04 | 233402424601108653 |         -9.99 |
| 28 | 2024-01-04 | 233402424601108753 |         -0.00 |
| 29 | 2024-01-04 | 233402424601134016 |         -7.00 |
