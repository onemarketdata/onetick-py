---
file_format: mystnb
---

# Takeout Success

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

Takeout success flags whether an order captured the available size in the market
on the touched side at arrival.

```text
TAKEOUT_SUCCESS = QTY_FILLED >= ASK_SIZE,  when SIDE = 'BUY'
TAKEOUT_SUCCESS = QTY_FILLED >= BID_SIZE,  when SIDE = 'SELL'
```

The prevailing quote at order arrival (`STATE = 'N'`) is joined "as of" to the order,
supplying the `ASK_SIZE` / `BID_SIZE` available at that instant.

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_LSE_SAMPLE', tick_type='ORDER')
order = order.where(order['STATE'] == 'N')
order = order[['ID', 'SIDE', 'QTY_FILLED']]
order['DIRECTION'] = order.if_else(order['SIDE'] == 'BUY', 1, -1)

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte = qte[['ASK_SIZE', 'BID_SIZE']]

data = otp.join_by_time([order, qte])

data['TAKEOUT_SUCCESS'] = data.if_else(
    ((data['DIRECTION'] == 1) & (data['QTY_FILLED'] >= data['ASK_SIZE'])) |
    ((data['DIRECTION'] == -1) & (data['QTY_FILLED'] >= data['BID_SIZE'])),
    1, 0)
data = data[['ID', 'TAKEOUT_SUCCESS']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |                    Time |                 ID | TAKEOUT_SUCCESS |
|---:|------------------------:|-------------------:|----------------:|
|  0 | 2024-01-03 08:08:07.502 | 233402424600089290 |               0 |
|  1 | 2024-01-03 08:27:05.220 | 233402424600163510 |               0 |
|  2 | 2024-01-03 08:53:19.681 | 233402424600242348 |               0 |
|  3 | 2024-01-03 08:53:19.685 | 233402424600242354 |               0 |
|  4 | 2024-01-03 08:53:19.691 | 233402424600242364 |               0 |
| .. |                     ... |                ... |             ... |
| 16 | 2024-01-03 15:00:06.170 | 233402424600990504 |               0 |
| 17 | 2024-01-03 15:21:17.758 | 233402424601076779 |               0 |
| 18 | 2024-01-03 15:31:59.347 | 233402424601108653 |               0 |
| 19 | 2024-01-03 15:32:01.227 | 233402424601108753 |               0 |
| 20 | 2024-01-03 15:38:33.230 | 233402424601134016 |               0 |
