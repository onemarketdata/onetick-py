---
file_format: mystnb
---

# Number Spreads

Each example is a self-contained script that can be run against the OneTick Cloud sample databases.

```{literalinclude} ../webapi_configuration.py
```

Number Spreads measures how far an order executes from the touch,
expressed in units of the arrival spread (Far Touch minus Near Touch).

```text
NUM_SPREADS = abs((VWAP - FT) / (FT - NT)), and NaN when (FT - NT) = 0
```

`NT`/`FT` are the arrival quote prices by side; `VWAP` is the order fill VWAP.  
The prevailing quote at arrival is joined "as of" to the order.

```python
import onetick.py as otp

order = otp.DataSource(db='S_ORDERS_LSE_SAMPLE', tick_type='ORDER')
order = order[['ID', 'SIDE', 'PRICE_FILLED', 'QTY_FILLED']]

qte = otp.DataSource(db='LSE_SAMPLE', tick_type='QTE')
qte = qte[['ASK_PRICE', 'BID_PRICE']]

joined = otp.join_by_time([order, qte])

data = joined.agg({
    'VWAP_PRICE': otp.agg.vwap('PRICE_FILLED', 'QTY_FILLED'),
    'ARRIVAL_ASK_PRICE': otp.agg.first('ASK_PRICE'),
    'ARRIVAL_BID_PRICE': otp.agg.first('BID_PRICE'),
    'SIDE': otp.agg.first('SIDE'),
}, group_by=['ID'])

data['NT'] = data.if_else(data['SIDE'] == 'BUY', data['ARRIVAL_BID_PRICE'], data['ARRIVAL_ASK_PRICE'])
data['FT'] = data.if_else(data['SIDE'] == 'BUY', data['ARRIVAL_ASK_PRICE'], data['ARRIVAL_BID_PRICE'])

data['NUM_SPREADS'] = data.if_else(
    (data['FT'] - data['NT']) != 0,
    abs((data['VWAP_PRICE'] - data['FT']) / (data['FT'] - data['NT'])),
    otp.nan
)
data = data[['ID', 'NUM_SPREADS']]

result = otp.run(data,
                 start=otp.dt(2024, 1, 3),
                 end=otp.dt(2024, 1, 4),
                 timezone='Europe/London',
                 symbols='VOD')
result
```

{.dataframe}
|    |       Time |                 ID | NUM_SPREADS |
|---:|-----------:|-------------------:|------------:|
|  0 | 2024-01-04 | 233402424600089290 |    1.000000 |
|  1 | 2024-01-04 | 233402424600163510 |    0.750000 |
|  2 | 2024-01-04 | 233402424600242327 |    0.750000 |
|  3 | 2024-01-04 | 233402424600242348 |    1.000000 |
|  4 | 2024-01-04 | 233402424600242354 |    1.000000 |
| .. |        ... |                ... |         ... |
| 25 | 2024-01-04 | 233402424601044819 |    1.000000 |
| 26 | 2024-01-04 | 233402424601076779 |    0.333333 |
| 27 | 2024-01-04 | 233402424601108653 |    1.000000 |
| 28 | 2024-01-04 | 233402424601108753 |    0.000000 |
| 29 | 2024-01-04 | 233402424601134016 |    0.500000 |
