---
file_format: mystnb
---

# Effective spread

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

To calculate the `effective spread` of your orders,
you'll need to first calculate the Volume Weighted Average Price (VWAP) for each order
and determine the prevailing mid-price (average of bid and ask prices) at the time of each order.  
Then, you can use the formula for the effective spread:

```text
EFFECTIVE_SPREAD = 2 * DIRECTION * EXEC_QTY * (VWAP - ARRIVAL_MID)
```

Joining Orders recorded in the sample Orders database to LSE Quotes.  
As the dataset refers to a single venue, the `QTE` table is used for benchmarking performance.  
The prevailing quote is associated with each order message
through an "as of" join with {func}`otp.join_by_time <onetick.py.join_by_time>`,
then the fill `VWAP`, `ARRIVAL_MID` and `DIRECTION` are aggregated per order ID.

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
    'VWAP_PRICE': otp.agg.vwap('PRICE_FILLED', 'QTY_FILLED'),
    'EXEC_COUNT': otp.agg.count(),
    'EXEC_QTY': otp.agg.sum('QTY_FILLED'),
    'ARRIVAL_MID': otp.agg.first('MID_PRICE'),
    'DIRECTION': otp.agg.first('DIRECTION'),
}, group_by=['ID'])

data = data.where(data['EXEC_QTY'] > 0)
data['EFFECTIVE_SPREAD'] = 2 * data['DIRECTION'] * data['EXEC_QTY'] * (data['VWAP_PRICE'] - data['ARRIVAL_MID'])

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |       Time |                 ID | VWAP_PRICE | EXEC_COUNT | EXEC_QTY |    ARRIVAL_MID | DIRECTION | EFFECTIVE_SPREAD |
|---:|-----------:|-------------------:|-----------:|-----------:|---------:|---------------:|----------:|-----------------:|
|  0 | 2024-01-04 | 233402424600089290 |      70.37 |          5 |     1987 |         70.390 |         1 |           -79.48 |
|  1 | 2024-01-04 | 233402424600163510 |      70.28 |          3 |     1214 |         70.290 |         1 |           -24.28 |
|  2 | 2024-01-04 | 233402424600242327 |      70.70 |          3 |      750 |         70.710 |         1 |           -15.00 |
|  3 | 2024-01-04 | 233402424600242348 |      70.73 |          2 |      750 |         70.710 |        -1 |           -30.00 |
|  4 | 2024-01-04 | 233402424600242354 |      70.73 |          2 |      750 |         70.710 |        -1 |           -30.00 |
| .. |        ... |                ... |        ... |        ... |      ... |            ... |       ... |              ... |
| 25 | 2024-01-04 | 233402424601044819 |      69.97 |          1 |     1423 |         69.975 |         1 |           -14.23 |
| 26 | 2024-01-04 | 233402424601076779 |      69.89 |          5 |       75 |         69.895 |        -1 |             0.75 |
| 27 | 2024-01-04 | 233402424601108653 |      69.95 |          3 |      999 |         69.955 |         1 |            -9.99 |
| 28 | 2024-01-04 | 233402424601108753 |      69.95 |          3 |        5 |         69.955 |        -1 |             0.05 |
| 29 | 2024-01-04 | 233402424601134016 |      69.86 |          3 |      700 |         69.860 |         1 |             0.00 |
