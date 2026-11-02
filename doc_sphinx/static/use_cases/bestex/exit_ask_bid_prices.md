---
file_format: mystnb
---

# Exit Ask Bid Prices for an Order

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

The exit price is the prevailing quote captured when an order leaves the book,
i.e. when it is fully executed (`STATE = 'F'`) or canceled (`STATE = 'C'`).

The prevailing quote is joined "as of" to each exit message; if an
order has more than one exit message, the first exit quote is taken per order ID (`group_by=['ID']`).

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_LSE_SAMPLE', tick_type='ORDER')
order = order.where((order['STATE'] == 'F') | (order['STATE'] == 'C'))
order = order[['ID']]

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte = qte[['ASK_PRICE', 'BID_PRICE']]

joined = otp.join_by_time([order, qte])

data = joined.agg({
    'EXIT_ASK_PRICE': otp.agg.first('ASK_PRICE'),
    'EXIT_BID_PRICE': otp.agg.first('BID_PRICE'),
}, group_by=['ID'])

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |       Time |                 ID | EXIT_ASK_PRICE | EXIT_BID_PRICE |
|---:|-----------:|-------------------:|---------------:|---------------:|
| 0  | 2024-01-04 | 233402424600089290 |          70.39 |          70.37 |
| 1  | 2024-01-04 | 233402424600163510 |          70.30 |          70.28 |
| 2  | 2024-01-04 | 233402424600242327 |          70.73 |          70.69 |
| 3  | 2024-01-04 | 233402424600242348 |          70.73 |          70.69 |
| 4  | 2024-01-04 | 233402424600242354 |          70.73 |          70.69 |
| .. |        ... |                ... |            ... |            ... |
| 25 | 2024-01-04 | 233402424601044819 |          69.98 |          69.97 |
| 26 | 2024-01-04 | 233402424601076779 |          69.89 |          69.88 |
| 27 | 2024-01-04 | 233402424601108653 |          69.96 |          69.95 |
| 28 | 2024-01-04 | 233402424601108753 |          69.95 |          69.93 |
| 29 | 2024-01-04 | 233402424601134016 |          69.88 |          69.86 |
