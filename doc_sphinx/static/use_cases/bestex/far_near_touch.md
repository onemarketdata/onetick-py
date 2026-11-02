---
file_format: mystnb
---

# Far Touch (FT) and Near Touch (NT)

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

To find the `Near Touch` (NT) and `Far Touch` (FT) prices for every order,
considering the definitions for buy and sell orders,
you'll need to process the order data to determine the arrival bid and ask prices,
and then calculate NT and FT based on the side of the order.

Far Touch (FT) and Near Touch (NT) are the arrival quote prices on the passive/aggressive side,
determined by the order side:

```text
BUY  ----> NT = ARRIVAL_BID_PRICE, FT = ARRIVAL_ASK_PRICE
SELL ----> NT = ARRIVAL_ASK_PRICE, FT = ARRIVAL_BID_PRICE
```

The arrival ask/bid are the prevailing quote at order arrival (`STATE = 'N'`), joined "as of" to the order.

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_LSE_SAMPLE', tick_type='ORDER')
order = order.where(order['STATE'] == 'N')
order = order[['ID', 'SIDE']]

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte['ARRIVAL_ASK_PRICE'] = qte['ASK_PRICE']
qte['ARRIVAL_BID_PRICE'] = qte['BID_PRICE']
qte = qte[['ARRIVAL_ASK_PRICE', 'ARRIVAL_BID_PRICE']]

data = otp.join_by_time([order, qte])

data['NT'] = data.if_else(data['SIDE'] == 'BUY', data['ARRIVAL_BID_PRICE'], data['ARRIVAL_ASK_PRICE'])
data['FT'] = data.if_else(data['SIDE'] == 'BUY', data['ARRIVAL_ASK_PRICE'], data['ARRIVAL_BID_PRICE'])
data = data[['ID', 'NT', 'FT']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |                    Time |                 ID |    NT |    FT |
|---:|------------------------:|-------------------:|------:|------:|
|  0 | 2024-01-03 08:08:07.502 | 233402424600089290 | 70.37 | 70.41 |
|  1 | 2024-01-03 08:27:05.220 | 233402424600163510 | 70.27 | 70.31 |
|  2 | 2024-01-03 08:53:19.681 | 233402424600242348 | 70.73 | 70.69 |
|  3 | 2024-01-03 08:53:19.685 | 233402424600242354 | 70.73 | 70.69 |
|  4 | 2024-01-03 08:53:19.691 | 233402424600242364 | 70.73 | 70.69 |
| .. |                     ... |                ... |   ... |   ... |
| 16 | 2024-01-03 15:00:06.170 | 233402424600990504 | 70.07 | 70.03 |
| 17 | 2024-01-03 15:21:17.758 | 233402424601076779 | 69.91 | 69.88 |
| 18 | 2024-01-03 15:31:59.347 | 233402424601108653 | 69.95 | 69.96 |
| 19 | 2024-01-03 15:32:01.227 | 233402424601108753 | 69.96 | 69.95 |
| 20 | 2024-01-03 15:38:33.230 | 233402424601134016 | 69.85 | 69.87 |
