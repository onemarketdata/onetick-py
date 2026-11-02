---
file_format: mystnb
---

# Notional Spread (Quoted Spread)

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

The notional spread values the quoted spread over the executed size of an order.

```text
NOTIONAL_SPREAD = EXEC_QTY * (ARRIVAL_ASK_PRICE - ARRIVAL_BID_PRICE)
```

The arrival ask/bid are the prevailing quote at order arrival, joined "as of" to the order.  
`EXEC_QTY` is the total filled quantity of the order.

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_LSE_SAMPLE', tick_type='ORDER')
order = order[['ID', 'QTY_FILLED']]

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte = qte[['ASK_PRICE', 'BID_PRICE']]

joined = otp.join_by_time([order, qte])

data = joined.agg({
    'EXEC_QTY': otp.agg.sum('QTY_FILLED'),
    'ARRIVAL_ASK_PRICE': otp.agg.first('ASK_PRICE'),
    'ARRIVAL_BID_PRICE': otp.agg.first('BID_PRICE'),
}, group_by=['ID'])

data['NOTIONAL_SPREAD'] = data['EXEC_QTY'] * (data['ARRIVAL_ASK_PRICE'] - data['ARRIVAL_BID_PRICE'])
data = data[['ID', 'NOTIONAL_SPREAD']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |       Time |                 ID | NOTIONAL_SPREAD |
|---:|-----------:|-------------------:|----------------:|
|  0 | 2024-01-04 | 233402424600089290 |           79.48 |
|  1 | 2024-01-04 | 233402424600163510 |           48.56 |
|  2 | 2024-01-04 | 233402424600242327 |           30.00 |
|  3 | 2024-01-04 | 233402424600242348 |           30.00 |
|  4 | 2024-01-04 | 233402424600242354 |           30.00 |
| .. |        ... |                ... |             ... |
| 25 | 2024-01-04 | 233402424601044819 |           14.23 |
| 26 | 2024-01-04 | 233402424601076779 |            2.25 |
| 27 | 2024-01-04 | 233402424601108653 |            9.99 |
| 28 | 2024-01-04 | 233402424601108753 |            0.05 |
| 29 | 2024-01-04 | 233402424601134016 |           14.00 |
