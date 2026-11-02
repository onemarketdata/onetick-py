---
file_format: mystnb
---

# Arrival Ask / Bid / Mid prices for an Order

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

In trading, the *arrival* of an order typically refers to the time when the order is first submitted,
usually indicated by an order `STATE = 'N'` (New).  
To find the *arrival price* for every order,
you need to find the prevailing quote at the time when each order was initially placed.  
The prevailing quote is associated with each arrival order through an "as of"
join with {func}`otp.join_by_time <onetick.py.join_by_time>`, and the mid is the average of the bid and ask.

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_LSE_SAMPLE', tick_type='ORDER')
order = order.where(order['STATE'] == 'N')
order = order[['ID']]

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte['ARRIVAL_ASK_PRICE'] = qte['ASK_PRICE']
qte['ARRIVAL_BID_PRICE'] = qte['BID_PRICE']
qte = qte[['ARRIVAL_ASK_PRICE', 'ARRIVAL_BID_PRICE']]

data = otp.join_by_time([order, qte])
data['ARRIVAL_MID_PRICE'] = (data['ARRIVAL_ASK_PRICE'] + data['ARRIVAL_BID_PRICE']) / 2
data = data[['ID', 'ARRIVAL_ASK_PRICE', 'ARRIVAL_BID_PRICE', 'ARRIVAL_MID_PRICE']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |                       Time |                 ID |   ARRIVAL_ASK_PRICE |   ARRIVAL_BID_PRICE |   ARRIVAL_MID_PRICE |
|---:|---------------------------:|-------------------:|--------------------:|--------------------:|--------------------:|
|  0 | 2024-01-03 08:08:07.502000 | 233402424600089290 |               70.41 |               70.37 |              70.390 |
|  1 | 2024-01-03 08:27:05.220000 | 233402424600163510 |               70.31 |               70.27 |              70.290 |
|  2 | 2024-01-03 08:53:19.681000 | 233402424600242348 |               70.73 |               70.69 |              70.710 |
|  3 | 2024-01-03 08:53:19.685000 | 233402424600242354 |               70.73 |               70.69 |              70.710 |
|  4 | 2024-01-03 08:53:19.691000 | 233402424600242364 |               70.73 |               70.69 |              70.710 |
| .. |                        ... |                ... |                 ... |                 ... |                 ... |
| 16 | 2024-01-03 15:00:06.170000 | 233402424600990504 |               70.07 |               70.03 |              70.050 |
| 17 | 2024-01-03 15:21:17.758000 | 233402424601076779 |               69.91 |               69.88 |              69.895 |
| 18 | 2024-01-03 15:31:59.347000 | 233402424601108653 |               69.96 |               69.95 |              69.955 |
| 19 | 2024-01-03 15:32:01.227000 | 233402424601108753 |               69.96 |               69.95 |              69.955 |
| 20 | 2024-01-03 15:38:33.230000 | 233402424601134016 |               69.87 |               69.85 |              69.860 |
