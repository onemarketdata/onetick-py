---
file_format: mystnb
---

# Right Way (Order Direction vs Mid Move)

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

*Right Way* metric flags whether each order was placed in the "right way" - i.e.
if the market mid price moved in the order's favor on the next quote update.

- *BUY*  ---> `RIGHT_WAY = 1` if the `NEXT_MID_PRICE > MID_PRICE`,
- *SELL* ---> `RIGHT_WAY = 1` if the `NEXT_MID_PRICE < MID_PRICE`.

`NEXT_MID_PRICE` is computed with `column[1]` (record-based lead) on the NBBO mid,
then the enhanced NBBO stream is joined "as of" to each order.

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_US_COMP_SAMPLE', tick_type='TRD')
order = order[['ID', 'SIDE']]

nbbo = otp.DataSource(db='US_COMP_SAMPLE', tick_type='NBBO')
nbbo['MID_PRICE'] = (nbbo['ASK_PRICE'] + nbbo['BID_PRICE']) / 2
nbbo['NEXT_MID_PRICE'] = (nbbo['ASK_PRICE'][1] + nbbo['BID_PRICE'][1]) / 2
nbbo = nbbo[['MID_PRICE', 'NEXT_MID_PRICE']]

data = otp.join_by_time([order, nbbo])

data['RIGHT_WAY'] = data.if_else(
    ((data['SIDE'] == 'BUY') & (data['NEXT_MID_PRICE'] > data['MID_PRICE'])) |
    ((data['SIDE'] == 'SELL') & (data['NEXT_MID_PRICE'] < data['MID_PRICE'])),
    1, 0)
data = data[['ID', 'SIDE', 'MID_PRICE', 'NEXT_MID_PRICE', 'RIGHT_WAY']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='America/New_York',
                 symbols='CSCO')
result
```

{.dataframe}
|     |                          Time |  ID | SIDE | MID_PRICE | NEXT_MID_PRICE | RIGHT_WAY |
|----:|------------------------------:|----:|-----:|----------:|---------------:|----------:|
|   0 | 2024-01-03 09:31:08.905124768 |   1 |  BUY |    50.125 |         50.125 |         0 |
|   1 | 2024-01-03 09:31:11.227255009 |   2 | SELL |    50.135 |         50.135 |         0 |
|   2 | 2024-01-03 09:32:08.022745023 |   3 |  BUY |    50.085 |         50.085 |         0 |
|   3 | 2024-01-03 09:32:55.320445732 |   4 |  BUY |    50.050 |         50.050 |         0 |
|   4 | 2024-01-03 09:34:55.054230281 |   5 |  BUY |    50.035 |         50.035 |         0 |
| ... |                           ... | ... |  ... |       ... |            ... |       ... |
| 240 | 2024-01-03 15:53:20.026170400 | 241 |  BUY |    50.525 |         50.525 |         0 |
| 241 | 2024-01-03 15:53:23.513196427 | 242 | SELL |    50.535 |         50.535 |         0 |
| 242 | 2024-01-03 15:53:51.117425606 | 243 | SELL |    50.495 |         50.495 |         0 |
| 243 | 2024-01-03 15:54:29.965268792 | 244 | SELL |    50.505 |         50.505 |         0 |
| 244 | 2024-01-03 15:54:52.546921833 | 245 | SELL |    50.475 |         50.475 |         0 |
