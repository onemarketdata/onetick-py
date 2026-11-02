---
file_format: mystnb
---

# Opportunity Cost

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

Opportunity cost values the unexecuted quantity against the price move over the life of the order
(arrival mid versus exit mid).

```text
OC = DIRECTION * UNEXEC_QTY * (ARRIVAL_MID - EXIT_MID)
```

Every order message is joined "as of" to the prevailing mid;
the first mid seen for the order is the arrival mid and the last is the exit mid.  
Unexecuted quantity is the summed difference between ordered and filled quantity.

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_LSE_SAMPLE', tick_type='ORDER')
order = order[['ID', 'QTY', 'QTY_FILLED', 'SIDE']]
order['DIRECTION'] = order.if_else(order['SIDE'] == 'BUY', 1, -1)
order['UNEXEC_QTY'] = order['QTY'] - order['QTY_FILLED']

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte['MID'] = (qte['ASK_PRICE'] + qte['BID_PRICE']) / 2
qte = qte[['MID']]

joined = otp.join_by_time([order, qte])

data = joined.agg({
    'ARRIVAL_MID': otp.agg.first('MID'),
    'EXIT_MID': otp.agg.last('MID'),
    'UNEXEC_QTY': otp.agg.sum('UNEXEC_QTY'),
    'DIRECTION': otp.agg.first('DIRECTION'),
}, group_by=['ID'])

data['OC'] = data['DIRECTION'] * data['UNEXEC_QTY'] * (data['ARRIVAL_MID'] - data['EXIT_MID'])
data = data[['ID', 'OC']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |       Time |                 ID |     OC |
|---:|-----------:|-------------------:|-------:|
|  0 | 2024-01-04 | 233402424600089290 | 347.67 |
|  1 | 2024-01-04 | 233402424600163510 |   0.00 |
|  2 | 2024-01-04 | 233402424600242327 |   0.00 |
|  3 | 2024-01-04 | 233402424600242348 |   0.00 |
|  4 | 2024-01-04 | 233402424600242354 |   0.00 |
| .. |        ... |                ... |    ... |
| 25 | 2024-01-04 | 233402424601044819 |   0.00 |
| 26 | 2024-01-04 | 233402424601076779 |-448.47 |
| 27 | 2024-01-04 | 233402424601108653 |   0.00 |
| 28 | 2024-01-04 | 233402424601108753 | -78.33 |
| 29 | 2024-01-04 | 233402424601134016 |  -0.22 |
